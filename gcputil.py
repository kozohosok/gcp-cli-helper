'''
  GCP utility for idempotent deployment with gcloud cli
'''

import os
import yaml
import zipfile as zf
from collections import Counter, defaultdict
from glob import iglob
from hashlib import sha256
from subprocess import PIPE, run

binfmt = 'C:/PATH-TO-GOOGLE-CLOUD-SDK/bin/%s.cmd'
cache_dir = './cache/'


def call(cmd, *args, stderr=PIPE, raw=False):
    print('call: ', cmd, *args)
    args = (binfmt % cmd,) + args
    res = run(args, stdout=PIPE, stderr=stderr, encoding='utf8')
    if raw:
        return res
    if res.stderr:
        print('stderr:', *res.stderr.rstrip().split('\n'), sep='\n  ')
    res.check_returncode()
    return res.stdout


def listBucket(parents):
    for prj,r in { (p, region.split('-', 1)[0]) for p,region in parents }:
        yield f"gs://{r}.artifacts.{prj}.appspot.com"
    pid, fmt = {}, '--format=value(projectNumber)'
    for p,region in parents:
        if p not in pid:
            pid[p] = call('gcloud', 'projects', 'describe', p, fmt).rstrip()
        yield f"gs://gcf-sources-{pid[p]}-{region}"


def updateRole(conf, cache):
    roles, oldroles = [ set(x.get('Role', [])) for x in (conf, cache) ]
    if roles == oldroles:
        return
    args = ['remove-iam-policy-binding',
      conf['ID'].split('@', 1)[1].split('.', 1)[0],
      '--quiet', '--member=serviceAccount:' + conf['ID']]
    for x in oldroles - roles:
        call('gcloud', 'projects', *(args + ['--role=' + x]))
    args[0] = 'add-iam-policy-binding'
    for x in roles - oldroles:
        call('gcloud', 'projects', *(args + ['--role=' + x]))
    return True


def flag(s):
    return '-' + ''.join( '-' + x.lower() if x.isupper() else x for x in s )


def flagOption(conf, key):
    for k,v in conf.get(key, {}).items():
        yield flag(k) + '=' + v


def labelOption(conf, cache, isdeploy):
    labels, oldlabels = conf.get('Label', {}), cache.get('Label', {})
    xs = set(oldlabels) - set(labels)
    if xs:
        yield '--remove-labels=' + ','.join(xs)
    xs = { f"{k}={v}" for k,v in labels.items() if v != oldlabels.get(k) }
    if xs:
        yield '--update-labels' if cache or isdeploy else '--labels'
        yield ','.join(xs)


def makeArgs(conf, mode, name=None):
    args = conf['Type'] + [mode, name or conf['ID']]
    args += flagOption(conf, 'Parent')
    return args + ['--quiet', '--format=yaml']


fixmode = dict(functions='deploy')

def updateResource(conf, cache):
    mode = fixmode.get(conf['Type'][0], 'update' if cache else 'create')
    args = makeArgs(conf, mode, not cache and conf.get('Name'))
    args += map(flag, conf.get('Flag', []))
    args += flagOption(conf, 'Create')
    args += flagOption(conf, 'Update')
    args += labelOption(conf, cache, mode == 'deploy')
    kwds = {} if conf.pop('PipeErr', True) else {'stderr': None}
    out = call('gcloud', *args, **kwds)
    print('stdout:', *out.rstrip().split('\n'), sep='\n  ')
    updateRole(conf, cache)
    return yaml.safe_load(out or call('gcloud', *makeArgs(conf, 'describe')))


def deleteResource(conf, path):
    call('gcloud', *makeArgs(conf, 'delete'))
    if os.path.isfile(path):
        os.remove(path)


def readCache(path):
    if not os.path.isfile(path):
        return {}, None
    with open(path) as f:
        data = yaml.safe_load(f)
    return data['Input'], data['Output']


def updateCache(name, conf, hold):
    cache_path = cache_dir + name
    cache, out = readCache(cache_path)
    diff = [ x != y for x,y in zip(cache.get('$hash', '??'), conf['$hash']) ]
    if cache and diff[0]:
        conf['ID'] += '-0' if cache['ID'] == conf['ID'] else ''
        out = updateResource(conf, {})
        hold['$bye'].append(cache)
    elif diff[1]:
        out = updateResource(conf, cache)
    elif not updateRole(conf, cache):
        print('  ---- not changed ----')
        return out
    hold[conf['Type'][0]].append(out['name'])
    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)
    with open(cache_path, 'w') as f:
        yaml.dump(dict(Input=conf, Output=out), f)
    return out


def clean(used, hold):
    for conf in hold['$bye'][::-1]:
        call('gcloud', *makeArgs(conf, 'delete'))
    for name in set(map(os.path.basename, iglob(cache_dir + '*'))) - used:
        path = cache_dir + name
        deleteResource(readCache(path)[0], path)
    parents = { tuple(x.split('/', 4)[1:4:2]) for x in hold['functions'] }
    for bucket in parents and listBucket(parents):
        call('gsutil', '-m', 'rm', '-r', bucket)


def traverse(el, **kwds):
    if isinstance(el, list):
        return [ traverse(x, **kwds) for x in el ]
    if not isinstance(el, dict):
        return el
    el = { k: traverse(v, **kwds) for k,v in el.items() }
    k = len(el) == 1 and min(el)
    x = kwds.get(k)
    return x[0](el[k], *x[1:]) if x else el


def mtime(x):
    return os.stat(x).st_mtime


def _zip(srcdst, files):
    src, dst = srcdst
    srcfiles, gsfile = list(iglob(src + '/*')), dst.startswith('gs://')
    zip = src + '.zip' if gsfile else dst
    files.add(zip)
    if os.path.isfile(zip) and max(map(mtime, srcfiles)) < mtime(zip):
        return dst
    n = len(src) + 1
    with zf.ZipFile(zip, 'w', zf.ZIP_DEFLATED) as z:
        for path in srcfiles:
            z.write(path, path[n:])
    if gsfile:
        call('gsutil', 'cp', zip, dst)
    return dst


def _sub(s, params, wait=None):
    xs = s.split('{')
    for i,x in enumerate(xs[1:], 1):
        x, y = x.split('}', 1)
        p = params.get(x)
        if p:
            xs[i] = p + y
        elif wait is not None:
            params[x] = wait.add(x)
    return ''.join(xs)


def _yml(srcdst, params, wait, files=None):
    src, dst = srcdst
    kwds = {} if files is None else {'_zip_': (_zip, files)}
    data = traverse(params['$data'][src], _sub_=(_sub, params, wait), **kwds)
    if wait or files is None:
        return dst
    files.add(dst)
    s = yaml.dump(data).encode('utf8')
    if os.path.isfile(dst):
        with open(dst, 'rb') as f:
            if sha256(f.read()).digest() == sha256(s).digest():
                return dst
    with open(dst, 'wb') as f:
        f.write(s)
    return dst


def makeDepend(conf, params):
    wait = set()
    traverse(conf, _sub_=(_sub, params, wait), _yml_=(_yml, params, wait))
    return { x.split('.', 1)[0] for x in wait }


def flatten(el, key=''):
    if isinstance(el, dict):
        xs = ( x for k,v in el.items() for x in flatten(v, f"{key}.{k}") )
    elif isinstance(el, list):
        xs = ( x for i,v in enumerate(el) for x in flatten(v, f"{key}[{i}]") )
    else:
        yield key, str(el)
        return
    for x in xs:
        yield x


def makeHash(conf, files):
    buf, i = [], conf['Type'][0] in fixmode
    for xs in (('Parent', 'Create'), ('Update', 'Label', 'Flag')):
        buf.append(sha256('|'.join( f"{x}{k}:{v}" for x in xs
          for k,v in sorted(flatten(conf.get(x, []))) ).encode('utf8')))
    for x in sorted(files):
        with open(x, 'rb') as f:
            buf[i].update(f"|file.{x}:".encode('utf8') + f.read())
    return [ x.hexdigest() for x in buf ]


def parse(name, conf, params, hold):
    print('_' * 79 + '\n')
    wait, files = set(), set()
    conf = traverse(conf, _yml_=(_yml, params, wait, files),
      _zip_=(_zip, files), _sub_=(_sub, params, wait))
    if wait:
        return print(name, 'needs', wait)
    conf['$hash'] = hash = makeHash(conf, files)
    print(f"{name} (hash)", *hash, sep='\n  ')
    out = updateCache(name, conf, hold)
    params.update( xs for xs in flatten(out, name) if xs[0] in params )
    params[name] = conf['ID']
    return True


def addAlias(el, data):
    if el is None:
        return data
    buf = set(el)
    if isinstance(data, dict):
        return { k: el.get(k) or data[k] for k in buf | set(data) }
    if isinstance(data, list):
        return el + [ x for x in data if x not in buf ]
    return el


def readConfig(path):
    print('template:', path)
    with open(path) as f:
        data = yaml.safe_load(f)
    params, specs = data.get('Parameters', {}), data['Resources']
    params['$data'], alias = data, data.get('Alias', {})
    for k,v in data.get('Defaults', {}).items():
        for conf in specs.values():
            conf[k] = addAlias(conf.get(k), v)
    for conf in specs.values():
        for x in conf.pop('Alias', []):
            for k,v in alias[x].items():
                conf[k] = addAlias(conf.get(k), v)
    return params, specs


def make(template_path):
    params, specs = readConfig(template_path)
    kick = defaultdict(set)
    for name,conf in specs.items():
        for x in makeDepend(conf, params):
            kick[x].add(name)
    deg = Counter( x for xs in kick.values() for x in xs )
    hold, work = defaultdict(list), set(specs) - set(deg)
    while work:
        name = work.pop()
        if parse(name, specs[name], params, hold):
            deg.subtract(kick.get(name, []))
            work |= { x for x in kick.pop(name, []) if not deg[x] }
    clean(set(specs) | set(kick), hold)
    if kick:
        return print('\n\nzombi', dict(kick))
    print('done')


def remove(template_path):
    params, specs = readConfig(template_path)
    kick = { name: makeDepend(conf, params) for name,conf in specs.items() }
    for name,conf in specs.items():
        conf['ID'] = params[name] = readCache(cache_dir + name)[0]['ID']
    deg = Counter( x for xs in kick.values() for x in xs )
    work = set(specs) - set(deg)
    while work:
        print('_' * 79 + '\n')
        name = work.pop()
        conf = traverse(specs[name], _sub_=(_sub, params))
        deleteResource(conf, cache_dir + name)
        deg.subtract(kick[name])
        work |= { x for x in kick[name] if not deg[x] }
    if any(deg.values()):
        return print('\n\nzombi', set(deg.elements()))
    print('done')

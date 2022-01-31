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
    roles, oldroles = ( set(x.get('Role', [])) for x in (conf, cache) )
    if roles == oldroles:
        return
    s = conf['ID']
    args = ['remove-iam-policy-binding', s.split('@', 1)[1].split('.', 1)[0],
      '--member=serviceAccount:' + s, '--quiet']
    for x in oldroles - roles:
        call('gcloud', 'projects', *(args + ['--role=' + x]))
    args[0] = 'add-iam-policy-binding'
    for x in roles - oldroles:
        call('gcloud', 'projects', *(args + ['--role=' + x]))
    return True


def flag(s):
    return '-' + ''.join( '-' + x.lower() if x.isupper() else x for x in s )


clearopt = {'BuildWorkerPool', 'MaxInstances', 'MinInstances', 'VpcConnector'}

def flagOption(conf, cache):
    keys, oldkeys = ( set(x.get('Update', [])) for x in (conf, cache) )
    for x in clearopt & oldkeys - keys:
        yield flag(f"Clear{x}")
    for x in conf.get('Flag', []):
        yield flag(x)


def flagValue(el, *key):
    return ( flag(k) + f"={v}" for x in key for k,v in el.get(x, {}).items() )


def tagValue(conf, cache, isupdate):
    tags, oldtags = conf.get('Tag', {}), cache.get('Tag', {})
    labels, oldlabels = tags.get('Labels', {}), oldtags.get('Labels', {})
    xs = set(oldlabels) - set(labels)
    if xs:
        yield '--remove-labels=' + ','.join(xs)
    xs = { f"{k}={v}" for k,v in labels.items() if v != oldlabels.get(k) }
    if xs:
        yield '--update-labels' if isupdate else '--labels'
        yield ','.join(xs)
    xs = set(tags) - {'Labels'}
    for x in set(oldtags) - {'Labels'} - xs:
        yield flag(f"Clear{x}")
    for x in xs:
        yield flag(f"Set{x}")
        yield ','.join( f"{k}={v}" for k,v in tags[x].items() )


def _gcloud(conf, mode, name=None, opts=(), **kwds):
    args = conf['Type'] + [mode, name or conf['ID']]
    args += flagValue(conf, 'Parent')
    args += ['--quiet', '--format=yaml']
    args += ( x for xs in opts for x in xs )
    return call('gcloud', *args, **kwds)


fixmode = dict(functions='deploy')

def updateResource(conf, cache):
    if cache:
        conf['ID'] = cache['ID']
    fix, create = fixmode.get(conf['Type'][0]), len(cache) < 2 and 'create'
    name = create and conf.get('Name')
    opts = [tagValue(conf, cache, fix or not create),
      flagValue(conf, 'Create', 'Update'), flagOption(conf, cache)]
    kwds = {} if conf.pop('PipeErr', True) else {'stderr': None}
    out = _gcloud(conf, fix or create or 'update', name, opts, **kwds)
    updateRole(conf, cache)
    return yaml.safe_load(out or _gcloud(conf, 'describe'))


def deleteResource(conf, cache_path):
    _gcloud(conf, 'delete')
    if os.path.isfile(cache_path):
        os.remove(cache_path)


def readCache(cache_path):
    if not os.path.isfile(cache_path):
        return {}, None
    with open(cache_path) as f:
        data = yaml.safe_load(f)
    return data['Input'], data['Output']


def updateCache(name, conf, hold, cache_path):
    cache, out = readCache(cache_path)
    diff = [ x != y for x,y in zip(cache.get('$hash', '??'), conf['$hash']) ]
    if any(diff):
        if cache and diff[0]:
            hold['$bye'].append(cache)
            idx = [ x['Type'] + [x['ID']] for x in (conf, cache) ]
            cache = {'ID': conf['ID'] + '-0'} if idx[0] == idx[1] else {}
        out = updateResource(conf, cache)
        hold[conf['Type'][0]].append(out['name'])
    elif not updateRole(conf, cache):
        print('  ---- not changed ----')
        conf['ID'] = cache['ID']
        return out
    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)
    with open(cache_path, 'w') as f:
        yaml.dump(dict(Input=conf, Output=out), f)
    return out


def clean(used, hold):
    for conf in hold['$bye'][::-1]:
        _gcloud(conf, 'delete')
    for name in set(map(os.path.basename, iglob(cache_dir + '*'))) - used:
        path = cache_dir + name
        deleteResource(readCache(path)[0], path)
    parents = { tuple(x.split('/', 4)[1:4:2]) for x in hold['functions'] }
    for bucket in parents and listBucket(parents):
        call('gsutil', '-m', 'rm', '-r', bucket)


def traverse(el, **kwds):
    t = type(el)
    if t is not dict:
        return [ traverse(x, **kwds) for x in el ] if t is list else el
    el = { k: traverse(v, **kwds) for k,v in el.items() }
    k = len(el) == 1 and min(el)
    x = kwds.get(k)
    return x[0](el[k], *x[1:]) if x else el


def mtime(path):
    return os.stat(path).st_mtime


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
        if p is not None:
            xs[i] = p + y
        elif wait is not None:
            params[x] = params[x.split('.', 1)[0]]
            wait.add(x)
    return ''.join(xs)


def _yml(srcdst, params, wait, files=None):
    src, dst = srcdst
    kwds = {} if files is None else {'_zip_': (_zip, files)}
    data = traverse(params['$data'][src], _sub_=(_sub, params, wait), **kwds)
    if wait or files is None:
        return dst
    files.add(dst)
    diff, s = True, yaml.dump(data).encode('utf8')
    if os.path.isfile(dst):
        with open(dst, 'rb') as f:
            diff = sha256(f.read()).digest() != sha256(s).digest()
    if diff:
        with open(dst, 'wb') as f:
            f.write(s)
    return dst


def makeDepend(conf, params):
    wait = set()
    traverse(conf, _sub_=(_sub, params, wait), _yml_=(_yml, params, wait))
    return { x.split('.', 1)[0] for x in wait }


def flatten(el, key=''):
    t = type(el)
    if t is dict:
        return ( x for k,v in el.items() for x in flatten(v, f"{key}.{k}") )
    if t is not list:
        return [(key, str(el))]
    key += '[]'
    return ( y for x in el for y in flatten(x, key) )


def makeHash(conf, files):
    buf, i = [], conf['Type'][0] in fixmode
    for xs in (('Parent', 'Create'), ('Update', 'Flag', 'Tag')):
        buf.append(sha256('|'.join( f"{x}{k}:{v}" for x in xs
          for k,v in sorted(flatten(conf.get(x, []))) ).encode('utf8')))
    for x in sorted(files):
        with open(x, 'rb') as f:
            buf[i].update(f"|file.{x}:".encode('utf8') + f.read())
    buf[0].update('/'.join(conf['Type'] + [conf['ID']]).encode('ascii'))
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
    out = updateCache(name, conf, hold, cache_dir + name)
    params.update( x for x in flatten(out, name) if x[0] in params )
    params[name] = conf['ID']
    return True


def addAlias(el, data):
    if el is None:
        return data
    buf, t = set(el), type(data)
    if t is dict:
        return { k: addAlias(el.get(k), data[k]) for k in buf | set(data) }
    return el + [ x for x in data if x not in buf ] if t is list else el


def readConfig(path):
    print('template:', path)
    with open(path) as f:
        data = yaml.safe_load(f)
    params, specs = data.get('Parameters', {}), data['Resources']
    params['$data'], alias = data, data.get('Alias', {})
    params.update( (x, None) for x in specs )
    buf = data.get('Defaults', {})
    for conf in specs.values():
        conf.update( (k, addAlias(conf.get(k), v)) for k,v in buf.items() )
        conf.update( (k, addAlias(conf.get(k), v))
          for x in conf.pop('Alias', []) for k,v in alias[x].items() )
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

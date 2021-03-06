'''
  GCP utility for idempotent deployment with gcloud cli
'''

import os
import json
import yaml
import zipfile as zf
from collections import Counter, defaultdict
from glob import iglob
from hashlib import sha256
from itertools import product
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


def flag(key, value=None, sep='-'):
    s = '-' + ''.join( sep + x.lower() if x.isupper() else x for x in key )
    return s if value is None else f"{s}={value}"


def flagGroup(el, *keys):
    return ( flag(k, v) for x in keys for k,v in el.get(x, {}).items() )


clearopt = {'BuildWorkerPool', 'MaxInstances', 'MinInstances', 'VpcConnector'}

def flagOption(conf, cache):
    keys, oldkeys = ( set(x.get('Update', [])) for x in (conf, cache) )
    for x in clearopt & oldkeys - keys:
        yield flag(f"Clear{x}")
    for x in conf.get('Flag', []):
        yield flag(x)


def bqflagValue(conf, cache, create):
    keys = ['Create', 'Update'] if create else ['Update']
    for k,v in ( kv for x in keys for kv in conf.get(x, {}).items() ):
        yield '--' + flag(k, v, '_')[2:]
    tags, oldtags = conf.get('Tag', {}), cache.get('Tag', {})
    labels, oldlabels = tags.get('Labels', {}), oldtags.get('Labels', {})
    for x in set(oldlabels) - set(labels):
        yield f"--clear_label={x}:{oldlabels[x]}"
    act = '' if create else 'set_'
    for k,v in set(labels.items()) - set(oldlabels.items()):
        yield f"--{act}label={k}:{v}"
    kv = tags.get('Schema')
    if kv:
        yield '--schema=' + ','.join( f"{k}:{v}" for k,v in kv )


def tagValue(conf, cache, create):
    tags, oldtags = conf.get('Tag', {}), cache.get('Tag', {})
    labels, oldlabels = tags.get('Labels', {}), oldtags.get('Labels', {})
    xs = set(oldlabels) - set(labels)
    if xs:
        yield '--remove-labels=' + ','.join(xs)
    kv = set(labels.items()) - set(oldlabels.items())
    if kv:
        yield '--labels' if create else '--update-labels'
        yield ','.join( f"{k}={v}" for k,v in kv )
    xs = set(tags) - {'Labels'}
    for x in set(oldtags) - {'Labels'} - xs:
        yield flag(f"Clear{x}")
    for x in xs:
        yield flag(f"Set{x}")
        yield ','.join( f"{k}={v}" for k,v in tags[x].items() )


def _gcloud(conf, mode, tmpname=None, opts=(), **kwds):
    args = conf['Type'] + [mode, tmpname or conf['ID']]
    args += flagGroup(conf, 'Parent')
    args += ['--quiet', '--format=yaml']
    args += ( x for xs in opts for x in xs )
    return call('gcloud', *args, **kwds)


def _bq(conf, mode, opts=(), **kwds):
    args = [dict(create='mk', delete='rm').get(mode, mode)]
    if args[0] != mode:
        args.append('--force=true')
    args += ( x for xs in opts for x in xs )
    args += ['--format=json', conf['ID']]
    return call('bq', *args, **kwds)


def prepareUnbind(conf):
    ctype, parent = conf['Type'], conf['Parent'].copy()
    mode = 'remove-' + ctype[-1]
    return [dict(Type=ctype[1:-1], Parent=parent), mode, parent.pop('ID')]


def _deleteBind(conf, mode):
    tags, args = conf['Tag'], prepareUnbind(conf)
    keys = [ k for k,v in tags.items() if type(v) is list ]
    opt = { flag(k, tags[k]) for k in set(tags) - set(keys) }
    for xs in product(*map(tags.get, keys)):
        _gcloud(*args, opts=[opt, map(flag, keys, xs)])


def deleteResource(conf, cache_path=None):
    f = dict(bigquery=_bq, _bind=_deleteBind).get(conf['Type'][0], _gcloud)
    f(conf, 'delete')
    if cache_path and os.path.isfile(cache_path):
        os.remove(cache_path)


def aslist(el, keys):
    for k in el and keys:
        x = el[k]
        yield x if type(x) is list else [x]


def _updateBind(conf, cache, create, kwds):
    src = conf['Tag'], cache.get('Tag', {})
    keys, opt = list(src[0]), set(flagGroup(conf, 'Update'))
    vals, oldvals = ( set(product(*aslist(x, keys))) - {()} for x in src )
    args, same = prepareUnbind(conf), vals & oldvals
    for xs in oldvals - same:
        _gcloud(*args, opts=[map(flag, keys, xs)])
    args[1] = 'add-' + args[1].split('-', 1)[1]
    for xs in vals - same:
        _gcloud(*args, opts=[opt, map(flag, keys, xs)], **kwds)
    args[1] = 'update-' + args[1][4:]
    for xs in [] if conf.get('Update') == cache.get('Update') else same:
        _gcloud(*args, opts=[opt, map(flag, keys, xs)], **kwds)
    return dict(id=conf['ID'])


fixmode = dict(functions='deploy')

def _updateGcloud(conf, cache, create, kwds):
    fix, name = fixmode.get(conf['Type'][0]), create and conf.get('Name')
    opts = [flagGroup(conf, 'Create', 'Update'), flagOption(conf, cache),
      tagValue(conf, cache, create and not fix)]
    out = _gcloud(conf, fix or create or 'update', name, opts, **kwds)
    return yaml.safe_load(out or _gcloud(conf, 'describe'))


def _updateBq(conf, cache, create, kwds):
    _bq(conf, create or 'update', [bqflagValue(conf, cache, create)], **kwds)
    return json.loads(_bq(conf, 'show'))


def updateResource(conf, cache):
    if cache:
        conf['ID'] = cache['ID']
    kwds = {} if conf.pop('PipeErr', True) else {'stderr': None}
    fs = dict(bigquery=_updateBq, _bind=_updateBind)
    f = fs.get(conf['Type'][0], _updateGcloud)
    return f(conf, cache, len(cache) < 2 and 'create', kwds)


def write(s, path):
    dir = os.path.dirname(path)
    if dir and not os.path.isdir(dir):
        os.makedirs(dir)
    with open(path, 'w') as f:
        f.write(s)


def read(path, mode='r'):
    with open(path, mode) as f:
        return f.read()


def readCache(path):
    data = yaml.safe_load(read(path))
    return data['Input'], data['Output']


def updateCache(path, conf, hold):
    cache, out = readCache(path) if os.path.isfile(path) else ({}, None)
    diff = [ x != y for x,y in zip(cache.get('$hash', '??'), conf['$hash']) ]
    if not any(diff):
        print('  ---- not changed ----')
        conf['ID'] = cache['ID']
        return out
    if cache and diff[0]:
        hold['$bye'].append(cache)
        idx = [ x['Type'] + [x['ID']] for x in (conf, cache) ]
        cache = {'ID': conf['ID'] + '-0'} if idx[0] == idx[1] else {}
    out = updateResource(conf, cache)
    hold[conf['Type'][0]].append(out.get('id') or out['name'])
    write(yaml.dump(dict(Input=conf, Output=out)), path)
    return out


def clean(used, hold):
    print('_' * 79 + '\n')
    for conf in hold['$bye'][::-1]:
        deleteResource(conf)
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
    arch = src + '.zip' if gsfile else dst
    files.add(arch)
    if os.path.isfile(arch) and max(map(mtime, srcfiles)) < mtime(arch):
        return dst
    n = len(src) + 1
    with zf.ZipFile(arch, 'w', zf.ZIP_DEFLATED) as z:
        for path in srcfiles:
            z.write(path, path[n:])
    if gsfile:
        call('gsutil', 'cp', arch, dst)
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
    s = yaml.dump(data)
    if not os.path.isfile(dst) or read(dst) != s:
        write(s, dst)
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
        buf[i].update(f"|file.{x}:".encode('utf8') + read(x, 'rb'))
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
    out = updateCache(cache_dir + name, conf, hold)
    params.update( x for x in flatten(out, name) if x[0] in params )
    params[name] = conf['ID']
    return True


def merge(el, data):
    if el is None:
        return data
    buf, t = set(el), type(data)
    if t is dict:
        return { k: merge(el.get(k), data.get(k)) for k in buf | set(data) }
    return el + [ x for x in data if x not in buf ] if t is list else el


def readConfig(path):
    print('template:', path)
    data = yaml.safe_load(read(path))
    params, specs = data.get('Parameters', {}), data['Resources']
    params['$data'], alias = data, data.get('Alias', {})
    params.update( (x, None) for x in specs )
    buf = data.get('Defaults', {})
    for conf in specs.values():
        for x in [buf] + list(map(alias.get, conf.pop('Alias', []))):
            conf.update( (k, merge(conf.get(k), v)) for k,v in x.items() )
    return params, specs


def make(template_path):
    params, specs = readConfig(template_path)
    kick, hold = defaultdict(set), defaultdict(list)
    for name,conf in specs.items():
        for x in makeDepend(conf, params):
            kick[x].add(name)
    deg = Counter( x for xs in kick.values() for x in xs )
    work = set(specs) - set(deg)
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

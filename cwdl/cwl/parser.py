import os
import yaml

from cwdl.cwl.bindings import new_process


def load_file(path, base_dir='.'):
    path = os.path.join(base_dir, path)
    with open(path) as fp:
        obj = yaml.load(fp)
    return resolve_imports(obj, os.path.dirname(path))


def resolve_imports(obj, base_dir):
    if isinstance(obj, dict) and 'import' in obj:
        return load_file(obj['import'], base_dir)
    if isinstance(obj, dict):
        return {k: resolve_imports(v, base_dir) for k, v in obj.iteritems()}
    if isinstance(obj, list):
        return [resolve_imports(i, base_dir) for i in obj]
    return obj


def load(path):
    return new_process(load_file(path))

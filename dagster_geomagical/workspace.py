import inspect
import pathlib
import sys

from dagster import repository, PipelineDefinition
from dagster.seven import import_module_from_path

@repository
def workspace():
    old_path = sys.path[:]
    if '' not in sys.path:
        sys.path.insert(0, '')
    # TODO support schedules too?
    objects = {'pipelines': {}}
    workspace_folder = pathlib.Path('.')
    for py_file in workspace_folder.glob('**/*.py'):
        if py_file.name == 'solids.py' or py_file.name == 'workspace.py':
            continue
        mod = import_module_from_path(py_file.stem, str(py_file.absolute()))
        for name, obj in inspect.getmembers(mod):
            if isinstance(obj, PipelineDefinition):
                objects['pipelines'][name] = obj
    sys.path = old_path
    return objects

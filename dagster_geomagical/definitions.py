import functools

from dagster import solid, pipeline, resource, ModeDefinition

from .make_app import make_app

def celery_solid(**options):
    # Set some defaults.
    options.setdefault('required_resource_keys', set()).add('celery_app')
    # Pull out some non-Dagster options.
    queue = options.pop('queue', None)

    def decorator(fn):
        @solid(**options)
        @functools.wraps(fn)
        def wrapper(context, *args, **kwargs):
            celery_args = fn(context, *args, **kwargs)
            if isinstance(celery_args, dict):
                args_options = {'kwargs': celery_args}
            else:
                args_options = {'args': celery_args}
            app = context.resources.celery_app
            sig = app.signature(f'tasks.{context.solid.name}', queue=queue, **args_options)
            result = sig.delay()
            # disable_sync_subtasks=False because it's for a totally different Celery setup so the usual deadlock arguments don't apply.
            ret = result.get(disable_sync_subtasks=False)
            return ret
        return wrapper
    return decorator


@resource
def celery_app(_):
    return make_app()


def celery_pipeline(**options):
    mode_defs = options.setdefault('mode_defs', [])
    if not mode_defs:
        mode_defs.append(ModeDefinition())
    for mode_def in mode_defs:
        mode_def.resource_defs['celery_app'] = celery_app
    return pipeline(**options)

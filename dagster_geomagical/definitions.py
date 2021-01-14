import functools
import os

import kombu
from celery import Celery
from dagster import solid


def celery_solid(**options):
    # Pull out some non-Dagster options.
    queue = options.pop('queue', None)
    timeout = options.pop('timeout', None)
    task_name = options.pop('task_name', None)
    # If we're in the GRPC daemon, don't initialize the app at all.
    if 'CELERY_BROKER' in os.environ:
        # Build the app and make it persistent for connection pooling.
        app = Celery(set_as_current=False, broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
        app.conf.worker_enable_remote_control = False
        app.conf.task_queues = [
            kombu.Queue(queue, no_declare=True),
        ]
        app.conf.redis_retry_on_timeout = True
        app.conf.redis_socket_keepalive = True
    else:
        app = None

    def decorator(fn):
        @solid(**options)
        @functools.wraps(fn)
        def wrapper(context, *args, **kwargs):
            gen = fn(context, *args, **kwargs)
            celery_args = next(gen)
            if isinstance(celery_args, dict):
                celery_args.setdefault('run_id', context.run_id)
                args_options = {'kwargs': celery_args}
            else:
                args_options = {'args': celery_args}

            sig =  app.signature(f'tasks.{task_name or context.solid.name}', queue=queue, exchange='', routing_key=queue, **args_options)
            context.log.info(f"Running task {sig}")
            result = sig.delay()
            context.log.info(f"Started task {result.id}")
            # disable_sync_subtasks=False because it's for a totally different Celery setup so the usual deadlock arguments don't apply.
            ret = result.get(timeout=timeout, disable_sync_subtasks=False)
            context.log.info("Got result"+repr(ret))

            yield gen.send(ret)
            yield from gen
        return wrapper
    return decorator

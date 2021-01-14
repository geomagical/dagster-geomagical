import functools
import inspect
import os

import kombu
from celery import Celery


class SolidCelery(Celery):
    """Helper subclass for Dagster solid workers to set up a Celery app."""
    def __init__(self, name, **kwargs):
        kwargs.setdefault('main', 'tasks')
        kwargs.setdefault('broker', os.environ['CELERY_BROKER'])
        kwargs.setdefault('backend', os.environ['CELERY_BACKEND'])
        super().__init__(**kwargs)
        # Disable all the extraneos RabbitMQ stuff.
        self.conf.worker_enable_remote_control = False
        self.conf.task_queues = [
            kombu.Queue(name, no_declare=True),
        ]
        # Leave things in the queue for autoscaling counts.
        self.conf.task_acks_late = True
        # No prefetching so autoscaling works better.
        self.conf.worker_prefetch_multiplier = 1
        # For results.
        self.conf.redis_retry_on_timeout = True
        self.conf.redis_socket_keepalive = True

    def task(self, fn=None, **kwargs):
        if fn is not None:
            return self.task()(fn)
        def decorator(fn):
            kwargs['bind'] = True
            @functools.wraps(fn)
            def wrapper(self, run_id, *args, **kwargs):
                frame = inspect.currentframe()
                arg_string = inspect.formatargvalues(*frame)
                del frame # Frame objects suck to leave live references to, kill it quickly.
                print(f"Got task {self.request.id} via {run_id}: {fn.__name__}{arg_string}")
                ret = fn(self, run_id, *args, **kwargs)
                print(f"Finished task {self.request.id} via {run_id}: {repr(ret)}")
                return ret
            return super().task(**kargs)(wrapper)
        return decorator

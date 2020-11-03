import os

from celery import Celery

def make_app():
    app = Celery('tasks', broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
    app.conf.task_queue_max_priority = 10
    app.conf.task_acks_late = True
    return app


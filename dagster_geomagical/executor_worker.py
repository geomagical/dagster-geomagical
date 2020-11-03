import os

from celery import Celery
from dagster_celery.tasks import create_task

app = Celery('dagster', broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
app.conf.task_queue_max_priority = 10
app.conf.task_acks_late = True

execute_plan = create_task(app)

if __name__ == '__main__':
    app.worker_main()

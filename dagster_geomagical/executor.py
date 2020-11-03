import os

from dagster import configured
import dagster_celery

@dagster_celery.celery_executor.configured
def celery_executor(_):
    return {
        'broker': os.environ['CELERY_BROKER'],
        'backend': os.environ['CELERY_BACKEND'],
    }

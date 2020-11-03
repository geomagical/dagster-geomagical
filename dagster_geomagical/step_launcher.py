import os
import pickle
import subprocess
import sys

from celery import Celery
from dagster import Field, StringSource, check, resource
from dagster.core.code_pointer import FileCodePointer, ModuleCodePointer
from dagster.core.definitions.reconstructable import (
    ReconstructablePipeline,
    ReconstructableRepository,
)
from dagster.core.definitions.step_launcher import StepLauncher, StepRunRef
from dagster.core.execution.api import create_execution_plan
from dagster.core.execution.context.system import SystemStepExecutionContext
from dagster.core.execution.context_creation_pipeline import PlanExecutionContextManager
from dagster.core.execution.plan.execute_step import core_dagster_event_sequence_for_step
from dagster.core.instance import DagsterInstance
from dagster.core.storage.file_manager import LocalFileHandle, LocalFileManager
from dagster.utils import raise_interrupts_immediately




app = Celery('tasks', broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
app.conf.task_queue_max_priority = 10
app.conf.task_acks_late = True

@resource(
    config_schema={},
)
def celery_step_launcher(context):
    return CeleryStepLauncher(**context.resource_config)


class CeleryStepLauncher(StepLauncher):
    """Launches each step via Celery."""

    def __init__(self):
        pass

    def launch_step(self, step_context, prior_attempts_count):
        import ipdb; ipdb.set_trace()
        yield None

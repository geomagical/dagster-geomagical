import os
from collections import namedtuple

from celery import Celery
from dagster import check
from dagster.cli.api import ExecuteRunArgs
from dagster.core.events import EngineEventData, EventMetadataEntry
# from dagster.core.execution.api import execute_run
# from dagster.core.host_representation import ExternalPipeline
# from dagster.core.instance import DagsterInstance
from dagster.core.host_representation.handle import GrpcServerRepositoryLocationHandle
from dagster.core.launcher import RunLauncher
from dagster.core.origin import PipelineGrpcServerOrigin, PipelinePythonOrigin
from dagster.serdes import ConfigurableClass, serialize_dagster_namedtuple, whitelist_for_serdes
# from dagster.utils.hosted_user_process import recon_pipeline_from_origin

app = Celery('tasks', broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
app.conf.task_acks_late = True

# @whitelist_for_serdes
# class MyExecuteRunArgs(namedtuple("_MyExecuteRunArgs", "pipeline pipeline_run_id instance_ref")):
#     def __new__(cls, pipeline, pipeline_run_id, instance_ref):
#         return super(MyExecuteRunArgs, cls).__new__(
#             cls,
#             pipeline=check.inst_param(pipeline_origin, "pipeline", Pipeline),
#             pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
#             instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
#         )


class CeleryRunLauncher(RunLauncher, ConfigurableClass):
    """This run launcher launches runs synchronously, in memory, and is intended only for test.

    Use the :py:class:`dagster.DefaultRunLauncher`.
    """

    def __init__(self, inst_data=None):
        self._inst_data = inst_data

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return CeleryRunLauncher(inst_data=inst_data)

    def launch_run(self, instance, run, external_pipeline):
        if isinstance(external_pipeline.get_origin(), PipelineGrpcServerOrigin):
            repository_location_handle = (
                external_pipeline.repository_handle.repository_location_handle
            )

            if not isinstance(repository_location_handle, GrpcServerRepositoryLocationHandle):
                raise DagsterInvariantViolationError(
                    "Expected RepositoryLocationHandle to be of type "
                    "GrpcServerRepositoryLocationHandle but found type {}".format(
                        type(repository_location_handle)
                    )
                )

            repository_name = external_pipeline.repository_handle.repository_name
            location_name = external_pipeline.repository_handle.repository_location_handle.location_name
            pipeline_origin = PipelinePythonOrigin(
                pipeline_name=external_pipeline.name,
                repository_origin=repository_location_handle.get_repository_python_origin(
                    repository_name
                ),
            )
        else:
            location_name = 'local'
            pipeline_origin = external_pipeline.get_origin()

        input_json = serialize_dagster_namedtuple(
            ExecuteRunArgs(
                pipeline_origin=pipeline_origin, pipeline_run_id=run.run_id, instance_ref=None,
            )
        )

        sig =  app.signature('launch_run', queue=location_name, args=(input_json,))
        result = sig.delay()
        instance.report_engine_event(
            "Started Celery task for pipeline (task id: {result.id}).".format(result=result),
            run,
            EngineEventData(metadata_entries=[
                EventMetadataEntry.text(result.id, "task_id"),
            ]),
        )

        return run


    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        check.not_implemented("Termination not supported.")

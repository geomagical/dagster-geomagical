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


class CeleryRunLauncher(RunLauncher, ConfigurableClass):
    """This run launcher launches runs synchronously, in memory, and is intended only for test.

    Use the :py:class:`dagster.DefaultRunLauncher`.
    """

    def __init__(self, inst_data=None):
        self._inst_data = inst_data
        self._apps = {}

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return CeleryRunLauncher(inst_data=inst_data)

    def _get_app(self, name):
        """Lazy initialize and cache Celery apps for each location."""
        app = self._apps.get(name)
        if app is None:
            # Reparse the broker URL for the different vhost.
            parts = urllib.parse.urlparse(os.environ['CELERY_BROKER'])
            parts.path = f"{name}-pipelines"
            broker_url = urllib.parse.urlunparse(parts)
            # Build the app and make it persistent for connection pooling.
            app = Celery(set_as_current=False, broker=broker_url, backend=os.environ['CELERY_BACKEND'])
            self._apps[name] = app
        return app

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

        app = self._get_app(location_name)
        sig =  app.signature('launch_run', args=(input_json,))
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

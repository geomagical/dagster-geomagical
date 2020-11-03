import os
import sys

from celery import Celery
from celery.result import allow_join_result
from dagster import check
from dagster.cli.api import ExecuteRunArgs
from dagster.core.errors import DagsterSubprocessError
from dagster.core.events import EngineEventData
from dagster.core.execution.api import execute_run_iterator
from dagster.core.host_representation import ExternalPipeline
from dagster.core.instance import DagsterInstance
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster.utils.hosted_user_process import recon_pipeline_from_origin
from dagster.serdes import (
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
)

app = Celery('tasks', broker=os.environ['CELERY_BROKER'], backend=os.environ['CELERY_BACKEND'])
app.conf.task_acks_late = True


@app.task(name='launch_run', bind=True)
def launch_run(self, input_json):
    args = check.inst(deserialize_json_to_dagster_namedtuple(input_json), ExecuteRunArgs)
    recon_pipeline = recon_pipeline_from_origin(args.pipeline_origin)

    with (
        DagsterInstance.from_ref(args.instance_ref) if args.instance_ref else DagsterInstance.get()
    ) as instance:
        buffer = []

        def send_to_buffer(event):
            buffer.append(serialize_dagster_namedtuple(event))

        _execute_run_command_body(self.request.id, recon_pipeline, args.pipeline_run_id, instance, send_to_buffer)

        for line in buffer:
            print(line)


def _execute_run_command_body(task_id, recon_pipeline, pipeline_run_id, instance, write_stream_fn):

    # we need to send but the fact that we have loaded the args so the calling
    # process knows it is safe to clean up the temp input file
    # write_stream_fn(ExecuteRunArgsLoadComplete())

    pipeline_run = instance.get_run_by_id(pipeline_run_id)

    try:
        with allow_join_result():
            for event in execute_run_iterator(recon_pipeline, pipeline_run, instance):
                write_stream_fn(event)
    except KeyboardInterrupt:
        instance.report_engine_event(
            message="Pipeline execution terminated by interrupt", pipeline_run=pipeline_run,
        )
    except DagsterSubprocessError as err:
        if not all(
            [err_info.cls_name == "KeyboardInterrupt" for err_info in err.subprocess_error_infos]
        ):
            instance.report_engine_event(
                "An exception was thrown during execution that is likely a framework error, "
                "rather than an error in user code.",
                pipeline_run,
                EngineEventData.engine_error(serializable_error_info_from_exc_info(sys.exc_info())),
            )
    except Exception:  # pylint: disable=broad-except
        instance.report_engine_event(
            "An exception was thrown during execution that is likely a framework error, "
            "rather than an error in user code.",
            pipeline_run,
            EngineEventData.engine_error(serializable_error_info_from_exc_info(sys.exc_info())),
        )
    finally:
        instance.report_engine_event(
            "Task for pipeline completed (task: {task_id}).".format(task_id=task_id), pipeline_run,
        )
import json

from dagster import OpExecutionContext, Output
from dagster_dbt import DbtCli, dbt_assets
from dateutil import parser

from ..constants import MANIFEST_PATH

with MANIFEST_PATH.open() as f:
    manifest = json.load(f)


@dbt_assets(manifest=manifest)
def my_dbt_assets(context: OpExecutionContext, dbt: DbtCli):
    for event in dbt.cli(["build"], context=context).stream_raw_events():
        for dagster_event in event.to_default_asset_events(manifest=manifest):
            if isinstance(dagster_event, Output):
                event_node_info = event.raw_event["data"]["node_info"]

                started_at = parser.isoparse(event_node_info["node_started_at"])
                completed_at = parser.isoparse(event_node_info["node_finished_at"])

                metadata = {
                    "Execution Started At": started_at.isoformat(timespec="seconds"),
                    "Execution Completed At": completed_at.isoformat(timespec="seconds"),
                    "Execution Duration": (completed_at - started_at).total_seconds(),
                }

                context.add_output_metadata(
                    metadata=metadata,
                    output_name=dagster_event.output_name,
                )

            yield dagster_event

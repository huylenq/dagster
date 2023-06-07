from __future__ import annotations

from dagster import job, op


@op(config_schema={"num": int})
def requires_config(context):
    return context.op_config


@job
def simple_config_job():
    requires_config()

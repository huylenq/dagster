# type: ignore
from __future__ import annotations

from dagster import job
from ops import example_one_op


@job
def example_one_job():
    example_one_op()

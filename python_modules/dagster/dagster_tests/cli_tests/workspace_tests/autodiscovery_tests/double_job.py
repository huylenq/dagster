from __future__ import annotations

from dagster import job


@job
def pipe_one():
    pass


@job
def pipe_two():
    pass

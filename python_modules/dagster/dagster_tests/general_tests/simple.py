from __future__ import annotations

from dagster import job, op


@op
def foo():
    pass


@job
def bar():
    foo()

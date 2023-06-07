from __future__ import annotations

from dagster import graph


@graph
def graph_one():
    pass


@graph
def graph_two():
    pass

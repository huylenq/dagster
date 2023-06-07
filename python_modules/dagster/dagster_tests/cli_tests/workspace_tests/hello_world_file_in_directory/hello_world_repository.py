# type: ignore
from __future__ import annotations

from dagster import repository
from src.jobs import hello_world_job


@repository
def hello_world_repository():
    return [hello_world_job]

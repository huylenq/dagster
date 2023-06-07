from __future__ import annotations

from dagster import repository


@repository
def single_repository():
    return []

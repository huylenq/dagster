from __future__ import annotations

from dagster import repository


@repository
def error_repo():
    a = None
    a()

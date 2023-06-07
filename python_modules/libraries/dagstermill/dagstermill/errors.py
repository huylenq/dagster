from __future__ import annotations

from dagster._core.errors import DagsterError


class DagstermillError(DagsterError):
    """Base class for errors raised by dagstermill."""

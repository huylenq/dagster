from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def gcs_bucket():
    yield "dagster-scratch-ccdfe1e"

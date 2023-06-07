from __future__ import annotations

from dagster import Definitions, asset


def _make_defs():
    @asset
    def an_asset():
        pass

    return Definitions(assets=[an_asset])


defs = _make_defs()

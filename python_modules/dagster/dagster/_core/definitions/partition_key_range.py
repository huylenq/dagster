from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from dagster._annotations import PublicAttr


class PartitionKeyRange(NamedTuple):
    # Inclusive on both sides
    start: PublicAttr[str]
    end: PublicAttr[str]

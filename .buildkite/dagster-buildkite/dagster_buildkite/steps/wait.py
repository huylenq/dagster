from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..utils import WaitStep


def build_wait_step() -> WaitStep:
    return "wait"

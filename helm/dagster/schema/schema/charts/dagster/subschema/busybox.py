from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from ...utils.kubernetes import ExternalImage


class Busybox(BaseModel):
    image: ExternalImage

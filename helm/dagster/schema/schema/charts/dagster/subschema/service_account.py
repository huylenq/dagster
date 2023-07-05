from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from ...utils import kubernetes


class ServiceAccount(BaseModel):
    create: bool
    name: str
    annotations: kubernetes.Annotations

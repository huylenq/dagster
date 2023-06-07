from __future__ import annotations

from pydantic import BaseModel


class Migrate(BaseModel):
    enabled: bool

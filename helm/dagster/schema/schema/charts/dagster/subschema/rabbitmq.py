from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from ...utils.kubernetes import ExternalImage


class RabbitMQConfiguration(BaseModel):
    username: str
    password: str


class Service(BaseModel):
    port: int


class VolumePermissions(BaseModel):
    enabled: bool = Field(default=True, const=True)
    image: ExternalImage


class RabbitMQ(BaseModel):
    enabled: bool
    image: ExternalImage
    rabbitmq: RabbitMQConfiguration
    service: Service
    volumePermissions: VolumePermissions

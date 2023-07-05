from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Union

from pydantic import BaseModel

if TYPE_CHECKING:
    from ...utils import kubernetes


class IngressPathType(str, Enum):
    EXACT = "Exact"
    PREFIX = "Prefix"
    IMPLEMENTATION_SPECIFIC = "ImplementationSpecific"


class IngressTLSConfiguration(BaseModel):
    enabled: bool
    secretName: str


# Enforce as HTTPIngressPath: see https://github.com/dagster-io/dagster/issues/3184
class IngressPath(BaseModel):
    path: str
    pathType: IngressPathType
    serviceName: str
    servicePort: Union[str, int]


class DagitIngressConfiguration(BaseModel):
    host: str
    path: str
    pathType: IngressPathType
    tls: IngressTLSConfiguration
    precedingPaths: List[IngressPath]
    succeedingPaths: List[IngressPath]


class FlowerIngressConfiguration(BaseModel):
    host: str
    path: str
    pathType: IngressPathType
    tls: IngressTLSConfiguration
    precedingPaths: List[IngressPath]
    succeedingPaths: List[IngressPath]


class Ingress(BaseModel):
    enabled: bool
    apiVersion: Optional[str]
    labels: kubernetes.Labels
    annotations: kubernetes.Annotations
    dagit: DagitIngressConfiguration
    readOnlyDagit: DagitIngressConfiguration
    flower: FlowerIngressConfiguration

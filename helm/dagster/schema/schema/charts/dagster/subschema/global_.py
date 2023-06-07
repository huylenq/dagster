from __future__ import annotations

from pydantic import BaseModel


class Global(BaseModel):
    postgresqlSecretName: str
    dagsterHome: str
    serviceAccountName: str
    celeryConfigSecretName: str

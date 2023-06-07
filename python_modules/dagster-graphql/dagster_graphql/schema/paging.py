from __future__ import annotations

import graphene


class GrapheneCursor(graphene.Int, graphene.Scalar):
    class Meta:
        name = "Cursor"

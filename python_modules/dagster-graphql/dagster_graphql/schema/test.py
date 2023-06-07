from __future__ import annotations

import graphene


class GrapheneTestFields(graphene.ObjectType):
    class Meta:
        name = "TestFields"

    alwaysException = graphene.String()

    def resolve_alwaysException(self, _):
        raise Exception("as advertised")

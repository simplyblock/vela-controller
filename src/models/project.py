from uuid import UUID

from ._base import Model


class Project(Model):
    organization: UUID
    name: str

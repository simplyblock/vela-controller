from typing import Self
from uuid import UUID

import fdb
import pydantic

from ._base import Model


class Project(Model):
    organization: UUID
    name: str

    @classmethod
    @fdb.transactional
    def by_organization(cls, tr, organization_id: UUID) -> list[Self]:
        return [
                project
                for project
                in cls.list(tr)
                if project.organization == organization_id
        ]


class ProjectCreate(pydantic.BaseModel):
    name: str

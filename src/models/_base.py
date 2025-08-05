import json
from typing import Self
from uuid import UUID, uuid4

import fdb
import pydantic

_PREFIX = 'objects'

fdb.api_version(730)


class Model(pydantic.BaseModel):
    id: UUID = pydantic.Field(default_factory=uuid4)

    @classmethod
    def _path(cls, id_: UUID | None = None) -> bytes:
        components = [_PREFIX, cls.__name__]
        if id_ is not None:
            components.append(str(id_))
        print(components)
        return '/'.join(components).encode()

    @classmethod
    def _from_db(cls, id_: UUID | str, encoded: bytes) -> Self:
        return cls.model_validate(dict(id=id_, **json.loads(encoded)))

    @classmethod
    @fdb.transactional
    def list(cls, tr) -> list[Self]:
        return [
            cls._from_db(key.decode().split('/')[-1], value)
            for key, value
            in tr.get_range_startswith(cls._path())
        ]

    @classmethod
    @fdb.transactional
    def get(cls, tr, id_: UUID) -> Self:
        encoded = tr[cls._path(id_)]
        if not encoded.present():
            raise KeyError(f'{cls.__name__} {id_} not present')
        return cls._from_db(id_, encoded)

    @classmethod
    @fdb.transactional
    def create(cls, tr, *args, **kwargs) -> Self:
        entity = cls(*args, **kwargs)
        entity.write(tr)
        return entity

    def _ipath(self) -> bytes:
        return self.__class__._path(self.id)

    @fdb.transactional
    def delete(self, tr):
        tr.clear(self._ipath())

    @fdb.transactional
    def read(self, tr):
        self = self._from_db(self.id, tr[self._ipath()])

    @fdb.transactional
    def write(self, tr):
        encoded = self.model_dump()
        del encoded['id']
        tr[self._ipath()] = json.dumps(encoded).encode()

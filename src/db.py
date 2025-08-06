import os
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session, create_engine

engine = create_engine(os.environ['POSTGRES_URL'])


def _get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session)]

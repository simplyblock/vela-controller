from fastapi import FastAPI

from . import models
from .api import api

app = FastAPI()
app.include_router(api)

__all__ = ['app', 'models']

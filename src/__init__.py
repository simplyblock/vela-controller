from fastapi import FastAPI

from . import models

app = FastAPI()

__all__ = ['app', 'models']

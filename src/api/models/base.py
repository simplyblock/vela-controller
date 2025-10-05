from ..db import *
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID as saUUID
from sqlalchemy.orm import relationship

# --- ORGANIZATION ---
class Organization(Base):
    __tablename__ = "organizations"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    max_backups = Column(Integer, nullable=False)

# --- PROJECT ---
class Project(Base):
    __tablename__ = "projects"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    organization = relationship("Organization")

# --- BRANCH ---
class Branch(Base):
    __tablename__ = "branches"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    project_id = Column(saUUID(as_uuid=True), ForeignKey("projects.id"), nullable=False)
    project = relationship("Project")
    env_type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    max_backups = Column(Integer, nullable=False)

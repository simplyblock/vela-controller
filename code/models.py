from sqlalchemy import Column, String, Boolean, DateTime, func, UUID as SQLAlchemyUUID
import uuid
from database import Base

class Deployment(Base):
    __tablename__ = "deployments"
    
    id = Column(SQLAlchemyUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    namespace = Column(String, nullable=False)
    release_name = Column(String, nullable=False)
    db_user = Column(String, nullable=False)
    db_name = Column(String, nullable=False)
    status = Column(String, default="pending")  # pending, deploying, running, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def to_dict(self):
        return {
            "id": str(self.id),
            "namespace": self.namespace,
            "release_name": self.release_name,
            "db_user": self.db_user,
            "db_name": self.db_name,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }

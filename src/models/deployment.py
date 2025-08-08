from typing import Optional
from datetime import datetime
from sqlmodel import Field, SQLModel

class Deployment(SQLModel, table=True):
    id: Optional[str] = Field(default=None, primary_key=True)
    namespace: str
    release_name: str
    db_user: str
    db_name: str
    status: str = "pending"  # pending, deploying, running, failed
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

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

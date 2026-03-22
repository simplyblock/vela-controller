from datetime import UTC, datetime

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session

from .branch import Branch
from .resources import ProvisioningLog, ResourceType

_AUDIT_FIELDS: dict[str, ResourceType] = {
    "milli_vcpu": ResourceType.milli_vcpu,
    "memory": ResourceType.ram,
    "iops": ResourceType.iops,
    "database_size": ResourceType.database_size,
    "storage_size": ResourceType.storage_size,
}


@event.listens_for(Session, "before_flush")
def _audit_branch_resource_changes(session, _flush_context, _instances):
    now = datetime.now(UTC)
    for obj in (*session.new, *session.dirty):
        if not isinstance(obj, Branch):
            continue
        action = "create" if obj in session.new else "update"
        mapper = inspect(obj)
        assert mapper is not None
        for field, resource_type in _AUDIT_FIELDS.items():
            history = mapper.attrs[field].load_history()
            if not history.has_changes():
                continue
            new_value = history.added[0] if history.added else None
            if new_value is None:
                continue
            session.add(
                ProvisioningLog(
                    branch_id=obj.id,
                    resource=resource_type,
                    amount=int(new_value),
                    action=action,
                    ts=now,
                )
            )

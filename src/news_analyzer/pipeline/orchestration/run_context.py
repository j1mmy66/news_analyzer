from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import uuid


@dataclass(frozen=True)
class RunContext:
    run_id: str
    started_at: datetime

    @classmethod
    def create(cls) -> "RunContext":
        return cls(run_id=str(uuid.uuid4()), started_at=datetime.now(timezone.utc))

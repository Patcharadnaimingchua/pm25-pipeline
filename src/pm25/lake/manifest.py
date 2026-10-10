"""Raw-landing manifest: one JSON per run that records exactly what was fetched.

It is the lineage anchor: fact row -> pipeline_run_id -> manifest -> source URL + checksum.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class FetchRecord:
    url: str
    object_key: str
    http_status: int
    bytes: int
    sha256: str
    fetched_at: str  # ISO UTC
    records: int | None = None
    error: str | None = None


@dataclass
class Manifest:
    source: str
    dataset: str
    run_id: str
    data_interval_start: str
    data_interval_end: str
    created_at: str
    files: list[FetchRecord] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add(self, record: FetchRecord) -> None:
        self.files.append(record)

    @property
    def ok_files(self) -> list[FetchRecord]:
        return [f for f in self.files if f.error is None and 200 <= f.http_status < 300]

    @property
    def failed_files(self) -> list[FetchRecord]:
        return [f for f in self.files if f not in self.ok_files]

    def to_json(self) -> bytes:
        body = asdict(self)
        body["summary"] = {
            "files": len(self.files),
            "ok": len(self.ok_files),
            "failed": len(self.failed_files),
            "bytes": sum(f.bytes for f in self.files),
            "records": sum(f.records or 0 for f in self.ok_files),
        }
        return json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")

"""Copy landed files into the lake raw zone (write-once) with a manifest, and summarise the ledger."""

from __future__ import annotations

import hashlib
import json
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from pm25.lake import paths
from pm25.lake.manifest import FetchRecord, Manifest, iso
from pm25.lake.s3 import put_raw

INDEX = "_pushed.jsonl"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def push_landing(*, source: str, dataset: str, landing_dir: Path, pattern: str, s3, bucket: str,
                 run_id: str, content_type: str, workers: int = 8) -> dict:
    """Upload files not pushed before. The local index (sha256 per file) prevents the same file
    from entering raw twice across push runs — duplicates are not real records."""
    index_path = landing_dir / INDEX
    pushed = set()
    if index_path.exists():
        pushed = {json.loads(line)["sha256"] for line in index_path.read_text(encoding="utf-8").splitlines() if line}
    now = datetime.now(timezone.utc)
    ingest_date = now.date()
    manifest = Manifest(source, dataset, run_id, iso(now), iso(now), iso(now))
    todo = []
    for f in sorted(landing_dir.glob(pattern)):
        if f.name.endswith(".part") or f.name == INDEX:
            continue
        digest = _sha256(f)
        if digest not in pushed:
            todo.append((f, digest))

    lock = threading.Lock()

    def upload(item):
        f, digest = item
        name = str(f.relative_to(landing_dir)).replace("/", "__").replace("\\", "__")
        key = paths.raw_key(source, dataset, ingest_date, run_id, name)
        data = f.read_bytes()
        put_raw(s3, bucket, key, data, content_type)
        # index right after each upload: a killed push must not make the next push upload everything again
        with lock, index_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"sha256": digest, "object_key": key}) + "\n")
        return FetchRecord(url=f"file://{f.as_posix()}", object_key=key, http_status=200, bytes=len(data),
                           sha256=digest, fetched_at=iso(datetime.now(timezone.utc)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        records = list(pool.map(upload, todo))
    for r in records:
        manifest.add(r)
    if records:
        put_raw(s3, bucket, paths.manifest_key(source, dataset, ingest_date, run_id), manifest.to_json(),
                "application/json")
    return {"uploaded": len(records), "skipped_already_pushed": len(pushed),
            "raw_prefix": paths.raw_prefix(source, dataset, ingest_date, run_id)}


def ledger_summary(ledger_dir: Path) -> list[dict]:
    """Per host: requests, errors, 429s, and the busiest minute/hour actually observed.
    Compare the peaks with the published limits — this table is the compliance evidence."""
    per_min: dict[str, Counter] = defaultdict(Counter)
    per_hour: dict[str, Counter] = defaultdict(Counter)
    per_day: dict[str, Counter] = defaultdict(Counter)
    totals: dict[str, Counter] = defaultdict(Counter)
    for path in sorted(Path(ledger_dir).glob("requests-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            host = e.get("host", "?")
            if e.get("event") == "robots":
                totals[host]["robots_disallow_events"] += 1
                continue
            if e.get("event") != "request":
                continue
            ts, w = e["ts"], int(e.get("weight", 1))
            per_min[host][ts[:16]] += w
            per_hour[host][ts[:13]] += w
            per_day[host][ts[:10]] += w
            totals[host]["requests"] += 1
            totals[host]["weighted"] += w
            totals[host]["bytes"] += int(e.get("bytes") or 0)
            status = int(e.get("status") or 0)
            totals[host]["http_429"] += status == 429
            totals[host]["errors"] += bool(e.get("error")) or status >= 400
    rows = []
    for host, t in sorted(totals.items()):
        rows.append({
            "host": host, "requests": t["requests"], "weighted": t["weighted"],
            "mb": round(t["bytes"] / 1e6, 1), "http_429": t["http_429"], "errors": t["errors"],
            "peak_per_minute": max(per_min[host].values(), default=0),
            "peak_per_hour": max(per_hour[host].values(), default=0),
            "peak_per_day": max(per_day[host].values(), default=0),
            "robots_disallow_events": t["robots_disallow_events"],
        })
    return rows

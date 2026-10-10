"""Shared helpers for the Week-1 verification scripts (run on a laptop with internet)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

REPORT = ROOT / "docs" / "data_volume_verification.md"


def load_dotenv(path: Path = ROOT / ".env") -> None:
    """Minimal .env reader so the scripts need no extra package."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() and not key.startswith("#"):
            os.environ.setdefault(key.strip(), value.strip())


def append_report(title: str, body: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    with REPORT.open("a", encoding="utf-8") as fh:
        fh.write(f"\n## {title} — {stamp}\n\n{body.strip()}\n")
    print(f"appended results to {REPORT.relative_to(ROOT)}")

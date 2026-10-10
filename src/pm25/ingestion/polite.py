"""Polite HTTP client: the single gate every pipeline request to an external host goes through.

What it enforces (rules come from config/source_policies.yaml):
  * only hosts that have a written policy can be called at all (unknown host -> refused)
  * a contact e-mail in the User-Agent (we can be reached if we cause trouble)
  * per-host pacing (requests/second) and concurrency
  * rolling budgets below each published limit (per minute / hour / day, weighted requests)
  * robots.txt: obeyed, or — for documented APIs — read and recorded with the reason we follow API terms
  * 429/503 + Retry-After honoured; exponential backoff with jitter otherwise
  * circuit breaker: after N consecutive failures stop calling that host for a cool-down period
  * an append-only ledger (JSONL) of every request: the evidence that we stayed within the rules
"""

from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
import yaml

from pm25.ingestion.http import RETRYABLE_STATUS, FatalHttpError, HttpResult, RetryableHttpError

log = logging.getLogger(__name__)

_POLICY_CANDIDATES = (
    Path("/opt/airflow/project-config/source_policies.yaml"),
    Path(__file__).resolve().parents[3] / "config" / "source_policies.yaml",
)


class FetchNotAllowed(RuntimeError):
    """The request breaks a policy (unknown host, host disabled, robots.txt, no contact)."""


class CircuitOpen(RetryableHttpError):
    """The host failed repeatedly; we stopped calling it for a while."""


class BudgetWaitTooLong(RetryableHttpError):
    """Our own budget is used up for longer than this caller may wait (e.g. an Airflow task with a
    timeout). Raised instead of sleeping, so the scheduler retries later and frees the worker slot."""


# ---------------------------------------------------------------- policies


@dataclass(frozen=True)
class Budget:
    limit: int
    window_s: float


@dataclass(frozen=True)
class HostPolicy:
    host: str
    name: str
    rate_per_sec: float
    max_concurrency: int
    budgets: tuple[Budget, ...] = ()
    weight: str = "1"  # "1" or "day_range"
    robots: str = "obey"  # "obey" | "api_terms"
    robots_reason: str = ""
    licence: str = ""
    attribution: str = ""

    @property
    def enabled(self) -> bool:
        return self.rate_per_sec > 0 and self.max_concurrency > 0


def parse_policies(doc: dict) -> tuple[dict[str, HostPolicy], bool]:
    hosts = {}
    for host, p in (doc.get("hosts") or {}).items():
        if p.get("robots", "obey") not in ("obey", "api_terms"):
            raise ValueError(f"{host}: robots must be 'obey' or 'api_terms'")
        if p.get("robots") == "api_terms" and not p.get("robots_reason"):
            raise ValueError(f"{host}: robots=api_terms needs a written robots_reason")
        hosts[host] = HostPolicy(
            host=host,
            name=p.get("name", host),
            rate_per_sec=float(p.get("rate_per_sec", 0)),
            max_concurrency=int(p.get("max_concurrency", 0)),
            budgets=tuple(Budget(int(b["limit"]), float(b["window_s"])) for b in p.get("budgets") or []),
            weight=str(p.get("weight", "1")),
            robots=p.get("robots", "obey"),
            robots_reason=" ".join(str(p.get("robots_reason", "")).split()),
            licence=p.get("licence", ""),
            attribution=p.get("attribution", ""),
        )
    return hosts, bool(doc.get("contact_required", True))


@lru_cache(maxsize=1)
def load_policies() -> tuple[dict[str, HostPolicy], bool]:
    for path in _POLICY_CANDIDATES:
        if path.exists():
            return parse_policies(yaml.safe_load(path.read_text(encoding="utf-8")))
    raise FileNotFoundError("config/source_policies.yaml not found")


# ---------------------------------------------------------------- budgets


class RollingBudget:
    """Several sliding windows at once (e.g. 480/min AND 4,000/hour AND 8,000/day)."""

    def __init__(self, budgets: tuple[Budget, ...], clock=time.monotonic):
        self.budgets = budgets
        self.clock = clock
        self.events: deque[tuple[float, int]] = deque()
        self.longest = max((b.window_s for b in budgets), default=0.0)

    def seed(self, ages_s_and_weights: list[tuple[float, int]]) -> None:
        """Pre-load requests made by earlier processes (from the ledger)."""
        now = self.clock()
        for age, weight in sorted(ages_s_and_weights, reverse=True):
            if age < self.longest:
                self.events.append((now - age, weight))

    def _used(self, window_s: float, now: float) -> int:
        return sum(w for t, w in self.events if t > now - window_s)

    def wait_time(self, weight: int) -> float:
        now = self.clock()
        while self.events and self.events[0][0] <= now - self.longest:
            self.events.popleft()
        wait = 0.0
        for b in self.budgets:
            if weight > b.limit:
                raise ValueError(f"request weight {weight} exceeds budget {b.limit}/{b.window_s}s")
            used = self._used(b.window_s, now)
            if used + weight <= b.limit:
                continue
            # free enough capacity: wait until the oldest events in this window expire
            need, freed = used + weight - b.limit, 0
            for t, w in self.events:
                if t > now - b.window_s:
                    freed += w
                    if freed >= need:
                        wait = max(wait, t + b.window_s - now)
                        break
        return wait

    def record(self, weight: int) -> None:
        self.events.append((self.clock(), weight))


# ---------------------------------------------------------------- ledger


class Ledger:
    """Append-only JSONL, one line per HTTP request, one file per month.
    Secrets (e.g. the FIRMS MAP_KEY, which sits inside the URL path) are masked before writing."""

    def __init__(self, directory: Path | str, secrets: tuple[str, ...] = ()):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._secrets = tuple(s for s in secrets if s and len(s) >= 6)

    def _mask(self, value):
        if isinstance(value, str):
            for s in self._secrets:
                value = value.replace(s, "***")
        return value

    def _path(self, when: datetime) -> Path:
        return self.dir / f"requests-{when:%Y-%m}.jsonl"

    def write(self, **entry) -> None:
        now = datetime.now(timezone.utc)
        entry = {"ts": now.isoformat(), **{k: self._mask(v) for k, v in entry.items()}}
        with self._lock, self._path(now).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def recent(self, host: str, max_age_s: float) -> list[tuple[float, int]]:
        """(age_seconds, weight) of requests to `host` within max_age_s, across month files."""
        now = datetime.now(timezone.utc)
        out = []
        for path in sorted(self.dir.glob("requests-*.jsonl"))[-2:]:
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("host") != host or e.get("event") != "request":
                    continue
                age = (now - datetime.fromisoformat(e["ts"])).total_seconds()
                if 0 <= age < max_age_s:
                    out.append((age, int(e.get("weight", 1))))
        return out


# ---------------------------------------------------------------- client


@dataclass
class _HostState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    semaphore: threading.Semaphore | None = None
    last_start: float = float("-inf")
    budget: RollingBudget | None = None
    consecutive_failures: int = 0
    open_until: float = 0.0
    robots: RobotFileParser | None = None
    robots_status: str = "unchecked"
    robots_logged: bool = False


def retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
    """Retry-After is either delta-seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


def contact_ok(user_agent: str) -> bool:
    return "@" in user_agent and "example.com" not in user_agent


class PoliteClient:
    def __init__(
        self,
        policies: dict[str, HostPolicy],
        user_agent: str,
        ledger: Ledger,
        *,
        contact_required: bool = True,
        session: requests.Session | None = None,
        clock=time.monotonic,
        sleep=time.sleep,
        max_retries: int = 5,
        base_backoff_s: float = 2.0,
        max_backoff_s: float = 300.0,
        breaker_threshold: int = 5,
        breaker_cooldown_s: float = 900.0,
        max_budget_wait_s: float | None = None,
    ):
        if contact_required and not contact_ok(user_agent):
            raise FetchNotAllowed("HTTP_USER_AGENT must contain a real contact e-mail (see .env.example)")
        self.policies = policies
        self.user_agent = user_agent
        self.ledger = ledger
        self.session = session or requests.Session()
        self.clock, self.sleep = clock, sleep
        self.max_retries, self.base_backoff_s, self.max_backoff_s = max_retries, base_backoff_s, max_backoff_s
        self.breaker_threshold, self.breaker_cooldown_s = breaker_threshold, breaker_cooldown_s
        self.max_budget_wait_s = max_budget_wait_s  # None = wait as long as needed (CLI scripts)
        self._states: dict[str, _HostState] = {}
        self._states_lock = threading.Lock()

    # -- per-host state
    def _state(self, policy: HostPolicy) -> _HostState:
        with self._states_lock:
            st = self._states.get(policy.host)
            if st is None:
                st = _HostState(semaphore=threading.Semaphore(policy.max_concurrency),
                                budget=RollingBudget(policy.budgets, self.clock))
                if policy.budgets:
                    st.budget.seed(self.ledger.recent(policy.host, st.budget.longest))
                self._states[policy.host] = st
            return st

    def policy_for(self, url: str) -> HostPolicy:
        host = urlparse(url).hostname or ""
        policy = self.policies.get(host)
        if policy is None:
            raise FetchNotAllowed(f"no policy for host {host!r}: add it to config/source_policies.yaml first")
        if not policy.enabled:
            raise FetchNotAllowed(f"{policy.name} is disabled by policy")
        return policy

    # -- robots.txt (RFC 9309: 4xx -> allow all, 5xx/unreachable -> treat as disallow all)
    def _robots_allows(self, policy: HostPolicy, st: _HostState, url: str) -> bool:
        if st.robots_status == "unchecked":
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = RobotFileParser()
            try:
                resp = self.session.get(robots_url, headers={"User-Agent": self.user_agent}, timeout=20)
                if resp.status_code >= 500:
                    rp.disallow_all, st.robots_status = True, f"robots {resp.status_code}: disallow all"
                elif resp.status_code >= 400:
                    rp.allow_all, st.robots_status = True, f"robots {resp.status_code}: allow all"
                else:
                    rp.parse(resp.text.splitlines())
                    st.robots_status = "parsed"
            except requests.RequestException as exc:
                rp.disallow_all, st.robots_status = True, f"robots unreachable ({type(exc).__name__}): disallow all"
            st.robots = rp
        return st.robots.can_fetch(self.user_agent, url)

    def _weight(self, policy: HostPolicy, weight: int | None) -> int:
        """How many 'calls' the server counts this request as.
        "1"           every request counts 1
        "day_range"   caller passes the number of days; each day counts 1 (FIRMS rule)
        "per_days:N"  caller passes the number of days; every N days count 1 (long ranges cost more)"""
        if policy.weight == "day_range":
            return max(1, int(weight or 1))
        if policy.weight.startswith("per_days:"):
            per = int(policy.weight.split(":", 1)[1])
            return max(1, math.ceil(int(weight or 1) / per))
        return 1

    def _backoff(self, attempt: int) -> float:
        return min(self.max_backoff_s, self.base_backoff_s * 2 ** attempt) * (0.5 + random.random() / 2)

    def get(self, url: str, *, params: dict | None = None, headers: dict | None = None,
            weight: int | None = None, timeout: float = 60.0) -> HttpResult:
        policy = self.policy_for(url)
        st = self._state(policy)
        host = policy.host

        allowed = self._robots_allows(policy, st, url)
        if not allowed:
            if policy.robots == "obey":
                self.ledger.write(event="robots", host=host, url=url, robots=st.robots_status,
                                  verdict="disallow", action="refused")
                raise FetchNotAllowed(f"robots.txt disallows {url}")
            if not st.robots_logged:  # record the decision once per host per process
                st.robots_logged = True
                self.ledger.write(event="robots", host=host, url=url, robots=st.robots_status,
                                  verdict="disallow", action="following documented API terms",
                                  reason=policy.robots_reason)
                log.info("robots.txt disallows %s; following API terms: %s", host, policy.robots_reason)

        if self.clock() < st.open_until:
            raise CircuitOpen(f"{policy.name}: circuit open after repeated failures; cooling down")

        w = self._weight(policy, weight)
        hdrs = {"User-Agent": self.user_agent, **(headers or {})}
        with st.semaphore:
            for attempt in range(self.max_retries + 1):
                with st.lock:  # pacing + budget, decided one request at a time per host
                    pace = max(0.0, st.last_start + 1.0 / policy.rate_per_sec - self.clock())
                    if pace:
                        self.sleep(pace)
                    wait = st.budget.wait_time(w)
                    if wait and self.max_budget_wait_s is not None and wait > self.max_budget_wait_s:
                        self.ledger.write(event="budget_wait_too_long", host=host, wait_s=round(wait),
                                          max_wait_s=self.max_budget_wait_s)
                        raise BudgetWaitTooLong(
                            f"{policy.name}: own budget used up for {wait:.0f}s (> {self.max_budget_wait_s:.0f}s);"
                            " retry later")
                    if wait:
                        log.info("%s budget reached; waiting %.0fs", policy.name, wait)
                        self.sleep(wait)
                    st.budget.record(w)
                    st.last_start = self.clock()

                status, body, err, resp = 0, b"", None, None
                try:
                    resp = self.session.get(url, params=params, headers=hdrs, timeout=timeout)
                    status, body = resp.status_code, resp.content
                except (requests.Timeout, requests.ConnectionError) as exc:
                    err = type(exc).__name__
                self.ledger.write(event="request", host=host, url=url, params=params, status=status,
                                  bytes=len(body), weight=w, attempt=attempt, error=err,
                                  robots="ok" if allowed else "disallowed; api terms followed")

                if err is None and status < 400:
                    st.consecutive_failures = 0
                    return HttpResult(url=resp.url, status=status, body=body,
                                      content_type=resp.headers.get("Content-Type", "application/octet-stream"))
                if err is None and status not in RETRYABLE_STATUS:
                    raise FatalHttpError(f"HTTP {status} for {url}: {body[:200]!r}")

                delay = None
                if resp is not None and status in (429, 503):
                    delay = retry_after_seconds(resp.headers.get("Retry-After"))
                if delay is None:
                    delay = self._backoff(attempt)
                if attempt < self.max_retries:
                    log.warning("%s: %s; retry %d in %.0fs", policy.name, err or f"HTTP {status}",
                                attempt + 1, delay)
                    self.sleep(min(delay, self.max_backoff_s))

        st.consecutive_failures += 1
        if st.consecutive_failures >= self.breaker_threshold:
            st.open_until = self.clock() + self.breaker_cooldown_s
            self.ledger.write(event="circuit_open", host=host, cooldown_s=self.breaker_cooldown_s)
        raise RetryableHttpError(f"{policy.name}: gave up after {self.max_retries + 1} attempts ({url})")


def default_client(ledger_dir: str | Path | None = None, **kwargs) -> PoliteClient:
    import os

    from pm25.settings import get_settings

    policies, contact_required = load_policies()
    s = get_settings()
    directory = ledger_dir or os.environ.get("PM25_LEDGER_DIR", "data/ledger")
    ledger = Ledger(directory, secrets=(s.firms_map_key or "", s.openaq_api_key or ""))
    return PoliteClient(policies, s.user_agent, ledger, contact_required=contact_required, **kwargs)

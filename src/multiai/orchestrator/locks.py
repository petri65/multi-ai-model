from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence

STATE_DIR = os.environ.get("MULTIAI_STATE_DIR") or "state"
DEFAULT_DB_PATH = os.path.join(STATE_DIR, "locks.sqlite")
DEFAULT_TTL_SECONDS = 15 * 60  # 15 minutes
DEFAULT_HEARTBEAT_SECONDS = 60
DEFAULT_ACQUIRE_TIMEOUT = 120  # seconds
DEFAULT_RETRY_INTERVAL = 1.0


class LeaseError(RuntimeError):
    """Base class for lease related failures."""


class LeaseTimeout(LeaseError):
    """Raised when leases could not be acquired before timeout."""


class LeaseNotHeld(LeaseError):
    """Raised when attempting to renew or release a lease that is not held."""


@dataclass(frozen=True)
class Lease:
    shard: str
    holder: str
    ttl: float
    heartbeat: float
    acquired_at: float
    updated_at: float
    expires_at: float


class LeaseManager:
    """SQLite-backed lease manager that enforces shard-level mutex semantics."""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        ttl: float = DEFAULT_TTL_SECONDS,
        heartbeat: float = DEFAULT_HEARTBEAT_SECONDS,
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        retry_interval: float = DEFAULT_RETRY_INTERVAL,
    ) -> None:
        self.db_path = db_path
        self.default_ttl = float(ttl)
        self.default_heartbeat = float(heartbeat)
        self.acquire_timeout = float(acquire_timeout)
        self.retry_interval = float(max(retry_interval, 0.1))
        self._lock = threading.RLock()
        self._ensure_parent()
        self._initialise()

    # -- private helpers -------------------------------------------------

    def _ensure_parent(self) -> None:
        parent = os.path.dirname(self.db_path) or "."
        os.makedirs(parent, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30,
            isolation_level=None,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _initialise(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leases (
                    shard TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    ttl REAL NOT NULL,
                    heartbeat_interval REAL NOT NULL,
                    acquired_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leases_holder ON leases(holder)"
            )

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def _now() -> float:
        return time.time()

    def _purge_expired(self, conn: sqlite3.Connection, now: Optional[float] = None) -> None:
        ts = now if now is not None else self._now()
        conn.execute("DELETE FROM leases WHERE expires_at <= ?", (ts,))

    @staticmethod
    def _row_to_lease(row: sqlite3.Row) -> Lease:
        return Lease(
            shard=row["shard"],
            holder=row["holder"],
            ttl=float(row["ttl"]),
            heartbeat=float(row["heartbeat_interval"]),
            acquired_at=float(row["acquired_at"]),
            updated_at=float(row["updated_at"]),
            expires_at=float(row["expires_at"]),
        )

    def _try_acquire(
        self,
        conn: sqlite3.Connection,
        shard: str,
        holder: str,
        ttl: Optional[float],
        heartbeat: Optional[float],
        now: float,
    ) -> Optional[Lease]:
        ttl_val = float(ttl if ttl is not None else self.default_ttl)
        heartbeat_val = float(heartbeat if heartbeat is not None else self.default_heartbeat)
        row = conn.execute("SELECT * FROM leases WHERE shard = ?", (shard,)).fetchone()
        if row is not None and row["holder"] != holder and row["expires_at"] > now:
            return None
        acquired_at = row["acquired_at"] if row is not None and row["holder"] == holder else now
        expires_at = now + ttl_val
        conn.execute(
            """
            INSERT INTO leases(shard, holder, ttl, heartbeat_interval, acquired_at, updated_at, expires_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(shard) DO UPDATE SET
                holder=excluded.holder,
                ttl=excluded.ttl,
                heartbeat_interval=excluded.heartbeat_interval,
                acquired_at=excluded.acquired_at,
                updated_at=excluded.updated_at,
                expires_at=excluded.expires_at
            """,
            (shard, holder, ttl_val, heartbeat_val, acquired_at, now, expires_at),
        )
        updated = conn.execute("SELECT * FROM leases WHERE shard = ?", (shard,)).fetchone()
        return self._row_to_lease(updated)

    # -- public API ------------------------------------------------------

    def acquire(
        self,
        shards: Sequence[str],
        holder: str,
        ttl: Optional[float] = None,
        heartbeat: Optional[float] = None,
        timeout: Optional[float] = None,
        retry_interval: Optional[float] = None,
    ) -> Dict[str, Lease]:
        """Acquire leases for a collection of shards."""
        unique_shards = sorted({s.strip() for s in shards if s and s.strip()})
        leases: Dict[str, Lease] = {}
        if not unique_shards:
            return leases

        deadline = self._now() + (timeout if timeout is not None else self.acquire_timeout)
        retry = retry_interval if retry_interval is not None else self.retry_interval

        last_error: Optional[str] = None
        while self._now() <= deadline:
            try:
                with self._transaction() as conn:
                    leases.clear()
                    now = self._now()
                    self._purge_expired(conn, now)
                    for shard in unique_shards:
                        lease = self._try_acquire(conn, shard, holder, ttl, heartbeat, now)
                        if lease is None:
                            last_error = (
                                f"shard '{shard}' currently held by another worker"
                            )
                            raise LeaseTimeout(last_error)
                        leases[shard] = lease
                return dict(leases)
            except LeaseTimeout:
                time.sleep(retry)
                continue
        raise LeaseTimeout(last_error or "timed out acquiring shard leases")

    def renew(
        self,
        shards: Sequence[str],
        holder: str,
        ttl: Optional[float] = None,
        heartbeat: Optional[float] = None,
    ) -> Dict[str, Lease]:
        unique_shards = sorted({s.strip() for s in shards if s and s.strip()})
        renewed: Dict[str, Lease] = {}
        if not unique_shards:
            return renewed

        with self._transaction() as conn:
            now = self._now()
            self._purge_expired(conn, now)
            for shard in unique_shards:
                row = conn.execute("SELECT * FROM leases WHERE shard = ?", (shard,)).fetchone()
                if row is None or row["holder"] != holder:
                    raise LeaseNotHeld(f"lease for shard '{shard}' not held by {holder}")
                ttl_val = float(ttl if ttl is not None else row["ttl"])
                heartbeat_val = float(heartbeat if heartbeat is not None else row["heartbeat_interval"])
                expires_at = now + ttl_val
                conn.execute(
                    "UPDATE leases SET ttl = ?, heartbeat_interval = ?, updated_at = ?, expires_at = ? WHERE shard = ?",
                    (ttl_val, heartbeat_val, now, expires_at, shard),
                )
                refreshed = conn.execute("SELECT * FROM leases WHERE shard = ?", (shard,)).fetchone()
                renewed[shard] = self._row_to_lease(refreshed)
        return renewed

    def release(self, shards: Sequence[str], holder: str) -> None:
        unique_shards = sorted({s.strip() for s in shards if s and s.strip()})
        if not unique_shards:
            return
        with self._transaction() as conn:
            now = self._now()
            self._purge_expired(conn, now)
            for shard in unique_shards:
                row = conn.execute("SELECT * FROM leases WHERE shard = ?", (shard,)).fetchone()
                if row is None:
                    continue
                if row["holder"] != holder:
                    raise LeaseNotHeld(
                        f"cannot release shard '{shard}' not held by {holder}"
                    )
                conn.execute("DELETE FROM leases WHERE shard = ?", (shard,))

    def active(self) -> List[Lease]:
        with self._transaction() as conn:
            now = self._now()
            self._purge_expired(conn, now)
            rows = conn.execute("SELECT * FROM leases").fetchall()
        return [self._row_to_lease(r) for r in rows]


_DEFAULT_MANAGER = LeaseManager()


def acquire(
    shards: Sequence[str],
    holder: str,
    ttl: Optional[float] = None,
    heartbeat: Optional[float] = None,
    timeout: Optional[float] = None,
    retry_interval: Optional[float] = None,
) -> Dict[str, Lease]:
    return _DEFAULT_MANAGER.acquire(shards, holder, ttl, heartbeat, timeout, retry_interval)


def renew(
    shards: Sequence[str],
    holder: str,
    ttl: Optional[float] = None,
    heartbeat: Optional[float] = None,
) -> Dict[str, Lease]:
    return _DEFAULT_MANAGER.renew(shards, holder, ttl, heartbeat)


def release(shards: Sequence[str], holder: str) -> None:
    _DEFAULT_MANAGER.release(shards, holder)


def active_leases() -> List[Lease]:
    return _DEFAULT_MANAGER.active()


__all__ = [
    "Lease",
    "LeaseError",
    "LeaseTimeout",
    "LeaseNotHeld",
    "LeaseManager",
    "acquire",
    "renew",
    "release",
    "active_leases",
]

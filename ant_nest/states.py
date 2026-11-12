"""File based persistent state for full/resume crawling.

The state lives in a single directory and is made of:

* ``state.db``/``state.db.bak``: a sqlite database holding processed request
  fingerprints, committed item keys, output file paths, run metadata and the
  last good checkpoint high water mark. The database is opened in WAL journal
  mode with synchronous writes, so a kill during a transaction never leaves a
  half written row, and ``state.db.bak`` is an explicit checkpoint backup used
  when the main database itself becomes unreadable.
* ``items/``: one json-lines file per item class, appended line by line and
  fsync-ed after every committed item, so already collected items survive a
  crash and a torn final line can be trimmed on the next start.

Only the standard library is used here.
"""
import ast
import asyncio
import enum
import hashlib
import logging
import os
import shutil
import sqlite3
import tempfile
import time
import typing
import uuid
from collections import defaultdict
from urllib.parse import urlencode

import httpx


logger = logging.getLogger(__name__)

STATE_DB_NAME = "state.db"
STATE_DB_BACKUP_NAME = "state.db.bak"
ITEMS_DIR_NAME = "items"


class Mode(enum.Enum):
    """Crawling mode controlled by settings."""

    FULL = "full"
    RESUME = "resume"


class StateCorruptedError(Exception):
    """Raise when no readable state can be recovered.

    It is raised instead of silently starting from an empty state, otherwise
    requests that were never processed could be skipped forever.
    """


def ast_literal_eval(raw: str) -> typing.Any:
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw


def request_fingerprint(request: httpx.Request) -> str:
    """Build a stable fingerprint for a request.

    The fingerprint only depends on the HTTP method and the URL. The query
    string is canonicalized by sorting the ``(key, value)`` pairs (repeated
    keys are kept), while the request body and headers are ignored, so two
    requests with the same method and URL are always considered duplicates.
    """
    pairs: typing.List[typing.Tuple[str, str]] = sorted(
        (str(key), str(value)) for key, value in request.url.params.multi_items()
    )
    canonical_url = str(request.url.copy_with(query=urlencode(pairs).encode()))
    raw = "{:s} {:s}".format(request.method.upper(), canonical_url)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class StateStorage:
    """Persistent, concurrency safe state shared by stateful pipelines.

    All writes are serialized with an asyncio lock because crawls run many
    coroutines concurrently. sqlite itself serializes the disk writes with WAL
    journaling, so every committed change is durable and a crash cannot leave
    a torn transaction.
    """

    def __init__(
        self,
        state_dir: str,
        mode: typing.Union[str, Mode] = Mode.RESUME,
        checkpoint_interval: int = 100,
    ):
        self.state_dir = os.path.abspath(state_dir)
        self.mode = mode if isinstance(mode, Mode) else Mode(str(mode).lower())
        self.checkpoint_interval = checkpoint_interval
        self.db_path = os.path.join(self.state_dir, STATE_DB_NAME)
        self.backup_path = os.path.join(self.state_dir, STATE_DB_BACKUP_NAME)
        self.items_dir = os.path.join(self.state_dir, ITEMS_DIR_NAME)
        self.logger = logging.getLogger(self.__class__.__name__)

        self._connection: typing.Optional[sqlite3.Connection] = None
        self._lock: typing.Optional[typing.Any] = None
        self._pending_requests: typing.Set[str] = set()
        self._done_requests: typing.Set[str] = set()
        self._item_keys: typing.Dict[str, typing.Set[str]] = defaultdict(set)
        self._output_paths: typing.Set[str] = set()
        self._writes_since_checkpoint = 0
        self.run_id = uuid.uuid4().hex

    # lifecycle --------------------------------------------------------
    async def open(self) -> "StateStorage":
        """Open or create the state directory according to :attr:`mode`.

        ``full`` wipes every historical fingerprint, item and checkpoint;
        ``resume`` restores the last good state and recovers corruption.
        """
        self._lock = asyncio.Lock()
        os.makedirs(self.state_dir, exist_ok=True)

        if self.mode is Mode.FULL and (
            os.path.exists(self.db_path) or os.path.exists(self.backup_path)
        ):
            self.logger.warning(
                "Reset persistent state under %s for a full crawl", self.state_dir
            )
            self.reset(self.state_dir)

        os.makedirs(self.items_dir, exist_ok=True)
        await self._open_database()
        await self._load_state()
        await self.checkpoint(force=True)
        await self.save_meta(
            run_id=self.run_id,
            mode=self.mode.value,
            started_at=time.time(),
            closed_at=None,
        )
        self.logger.info(
            "State storage opened at %s in %s mode (run %s)",
            self.state_dir,
            self.mode.value,
            self.run_id,
        )
        return self

    async def close(self):
        """Flush the final checkpoint and close the database."""
        if self._connection is None:
            return
        await self.save_meta(
            run_id=self.run_id,
            mode=self.mode.value,
            started_at=await self.get_meta("started_at", time.time()),
            closed_at=time.time(),
        )
        await self.checkpoint(force=True)
        self._connection.close()
        self._connection = None
        self.logger.info("State storage closed at %s", self.state_dir)

    async def __aenter__(self) -> "StateStorage":
        return await self.open()

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()

    @classmethod
    def reset(cls, state_dir: str):
        """Remove framework owned state files for a full rerun.

        Item output files previously tracked by the state are removed too,
        so no old fingerprint or checkpoint can leak into the new crawl.
        Only files the framework itself creates are touched, so unrelated
        files placed in the state directory are preserved.
        """
        if not state_dir or os.path.abspath(state_dir) == os.path.sep:
            raise ValueError("Refuse to reset an empty or root state directory")
        db_path = os.path.join(state_dir, STATE_DB_NAME)
        if os.path.exists(db_path):
            for path in cls._read_output_paths(db_path):
                if os.path.exists(path):
                    os.remove(path)
        for name in (
            STATE_DB_NAME,
            STATE_DB_BACKUP_NAME,
            ITEMS_DIR_NAME,
        ):
            path = os.path.join(state_dir, name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            elif os.path.exists(path):
                os.remove(path)
        for name in os.listdir(state_dir) if os.path.exists(state_dir) else []:
            if (
                name.startswith(".state-")
                or ".corrupt-" in name
                or name.endswith(("-wal", "-shm"))
            ):
                path = os.path.join(state_dir, name)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

    @staticmethod
    def _read_output_paths(db_path: str) -> typing.List[str]:
        try:
            connection = sqlite3.connect(db_path)
        except sqlite3.DatabaseError:
            return []
        try:
            rows = connection.execute("SELECT path FROM outputs").fetchall()
        except sqlite3.DatabaseError:
            rows = []
        finally:
            connection.close()
        return [row[0] for row in rows]

    # database ---------------------------------------------------------
    async def _open_database(self):
        if not os.path.exists(self.db_path):
            if os.path.exists(self.backup_path):
                self.logger.warning(
                    "State database missing, restore it from checkpoint %s",
                    self.backup_path,
                )
                shutil.copyfile(self.backup_path, self.db_path)
            else:
                self.logger.info("Create new state database %s", self.db_path)

        if not self._database_is_readable(self.db_path):
            quarantine_path = "{:s}.corrupt-{:d}".format(self.db_path, int(time.time()))
            os.replace(self.db_path, quarantine_path)
            for suffix in ("-wal", "-shm"):
                sidecar_path = self.db_path + suffix
                if os.path.exists(sidecar_path):
                    os.remove(sidecar_path)
            self.logger.error(
                "State database %s is corrupted and moved to %s",
                self.db_path,
                quarantine_path,
            )
            if os.path.exists(self.backup_path) and self._database_is_readable(
                self.backup_path
            ):
                self.logger.warning(
                    "Recover state from last good checkpoint %s", self.backup_path
                )
                shutil.copyfile(self.backup_path, self.db_path)
            else:
                raise StateCorruptedError(
                    "State database is corrupted and no readable checkpoint "
                    "backup is available; inspect {:s} and clean the state "
                    "directory explicitly before a full rerun".format(quarantine_path)
                )

        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        self._connection = connection
        self._create_tables()
        connection.commit()

    @staticmethod
    def _database_is_readable(path: str) -> bool:
        try:
            connection = sqlite3.connect(path)
            result = connection.execute("PRAGMA integrity_check").fetchone()
            connection.close()
        except sqlite3.DatabaseError:
            return False
        return result is not None and result[0] == "ok"

    def _create_tables(self):
        assert self._connection is not None
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS requests (
                fingerprint TEXT PRIMARY KEY,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS items (
                key TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outputs (
                path TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )

    async def _load_state(self):
        assert self._connection is not None
        self._done_requests = {
            row[0]
            for row in self._connection.execute("SELECT fingerprint FROM requests")
        }
        self._item_keys = defaultdict(set)
        for namespace, key in self._connection.execute(
            "SELECT namespace, key FROM items"
        ):
            self._item_keys[namespace].add(key)
        self._output_paths = {
            row[0] for row in self._connection.execute("SELECT path FROM outputs")
        }
        self._writes_since_checkpoint = 0

    # checkpoint -------------------------------------------------------
    async def checkpoint(self, force: bool = False):
        """Persist a complete, independently readable checkpoint backup.

        The sqlite connection is checkpointed into a temporary file inside the
        state directory and the temp file is atomically renamed over
        ``state.db.bak``; readers therefore never observe a half written
        backup.
        """
        assert self._connection is not None and self._lock is not None
        async with self._lock:
            self._checkpoint_locked(force=force)

    def _checkpoint_locked(self, force: bool = False):
        """Checkpoint implementation for callers already holding the lock."""
        assert self._connection is not None
        if not force and self._writes_since_checkpoint == 0:
            return
        self._connection.commit()
        fd, temp_path = tempfile.mkstemp(
            prefix=".state-", suffix=".tmp", dir=self.state_dir
        )
        os.close(fd)
        try:
            backup_connection = sqlite3.connect(temp_path)
            try:
                self._connection.backup(backup_connection)
            finally:
                backup_connection.close()
            self._fsync_file(temp_path)
            os.replace(temp_path, self.backup_path)
            self._fsync_directory(self.state_dir)
            self._writes_since_checkpoint = 0
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    async def _after_write(self):
        self._writes_since_checkpoint += 1
        if self._writes_since_checkpoint >= self.checkpoint_interval:
            self._checkpoint_locked(force=True)

    @staticmethod
    def _fsync_file(path: str):
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _fsync_directory(path: str):
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        except OSError:
            # fsync on a directory is unsupported on some platforms
            pass
        finally:
            os.close(descriptor)

    # request fingerprints ---------------------------------------------
    def is_request_known(self, fingerprint: str) -> bool:
        return (
            fingerprint in self._done_requests or fingerprint in self._pending_requests
        )

    def add_pending_request(self, fingerprint: str):
        """Remember a request in memory until it is really processed.

        Pending fingerprints are intentionally not persisted: only requests
        whose response pipelines succeed are marked done, so a killed process
        never skips a request that was never completed.
        """
        self._pending_requests.add(fingerprint)

    def discard_pending_request(self, fingerprint: str):
        self._pending_requests.discard(fingerprint)

    async def mark_request_done(self, fingerprint: str):
        assert self._connection is not None and self._lock is not None
        async with self._lock:
            self._pending_requests.discard(fingerprint)
            if fingerprint in self._done_requests:
                return
            self._connection.execute(
                "INSERT OR IGNORE INTO requests(fingerprint, created_at) "
                "VALUES(?, ?)",
                (fingerprint, time.time()),
            )
            self._connection.commit()
            self._done_requests.add(fingerprint)
            await self._after_write()

    # item idempotency -------------------------------------------------
    def is_item_committed(
        self, key: str, namespace: typing.Optional[str] = None
    ) -> bool:
        if namespace is None:
            return any(key in keys for keys in self._item_keys.values())
        return key in self._item_keys.get(namespace, set())

    async def commit_item(self, key: str, namespace: str):
        assert self._connection is not None and self._lock is not None
        async with self._lock:
            if key in self._item_keys[namespace]:
                return
            self._connection.execute(
                "INSERT OR IGNORE INTO items(key, namespace, created_at) "
                "VALUES(?, ?, ?)",
                (key, namespace, time.time()),
            )
            self._connection.commit()
            self._item_keys[namespace].add(key)
            await self._after_write()

    async def recover_items(self, namespace: str, keys: typing.Iterable[str]):
        """Merge item keys recovered from one output file into the state.

        Namespaces are handled independently, so repairing several output
        files never wipes keys recovered from another namespace.
        """
        assert self._connection is not None and self._lock is not None
        async with self._lock:
            recovered = set(keys)
            previous = set(self._item_keys.pop(namespace, set()))
            # Drop keys whose output line was torn away during repair, so the
            # item can be collected again instead of being skipped forever.
            stale_keys = previous - recovered
            for key in stale_keys:
                self._connection.execute(
                    "DELETE FROM items WHERE namespace=? AND key=?",
                    (namespace, key),
                )
            self._item_keys[namespace] = previous - stale_keys
            for key in keys:
                if key in self._item_keys[namespace]:
                    continue
                self._connection.execute(
                    "INSERT OR IGNORE INTO items(key, namespace, created_at) "
                    "VALUES(?, ?, ?)",
                    (key, namespace, time.time()),
                )
                self._item_keys[namespace].add(key)
            self._connection.commit()
            await self._after_write()

    # output files -----------------------------------------------------
    async def register_output(self, path: str, namespace: str):
        assert self._connection is not None and self._lock is not None
        absolute_path = os.path.abspath(path)
        if absolute_path in self._output_paths:
            return absolute_path
        async with self._lock:
            if absolute_path in self._output_paths:
                return absolute_path
            self._connection.execute(
                "INSERT OR IGNORE INTO outputs(path, namespace, created_at) "
                "VALUES(?, ?, ?)",
                (absolute_path, namespace, time.time()),
            )
            self._connection.commit()
            self._output_paths.add(absolute_path)
            await self._after_write()
        return absolute_path

    # run metadata and high water mark -------------------------------
    async def save_meta(self, **kwargs: typing.Any):
        assert self._connection is not None and self._lock is not None
        async with self._lock:
            for key, value in kwargs.items():
                self._connection.execute(
                    "INSERT INTO meta(key, value, updated_at) VALUES(?, ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                    "updated_at=excluded.updated_at",
                    (key, repr(value), time.time()),
                )
            self._connection.commit()

    async def get_meta(self, key: str, default: typing.Any = None) -> typing.Any:
        assert self._connection is not None
        row = self._connection.execute(
            "SELECT value FROM meta WHERE key=?", (key,)
        ).fetchone()
        if row is None:
            return default
        return ast_literal_eval(row[0])

    async def set_high_water_mark(self, value: typing.Any):
        await self.save_meta(high_water_mark=value)

    async def get_high_water_mark(self, default: typing.Any = 0) -> typing.Any:
        return await self.get_meta("high_water_mark", default)

    # introspection helpers ------------------------------------------
    @property
    def processed_request_count(self) -> int:
        return len(self._done_requests)

    @property
    def committed_item_count(self) -> int:
        return sum(len(keys) for keys in self._item_keys.values())


__all__ = [
    "Mode",
    "StateCorruptedError",
    "StateStorage",
    "request_fingerprint",
]

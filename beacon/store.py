import asyncio
import base64
import json
import sqlite3
import time
from typing import Any


class Store:
    """Stores objects in memory and persists them to SQLite."""

    def __init__(self, db_path: str):
        # WAL mode lets readers and writers work at the same time.
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS objects (
                key TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                value TEXT,
                labels TEXT NOT NULL,
                deleted INTEGER NOT NULL
            )
            """
        )
        # This index makes the cleanup task fast.
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_deleted_timestamp ON objects(deleted, timestamp)"
        )
        self._objects: dict[str, dict[str, Any]] = {}
        self._load_from_db()
        # SQLite can process only one write at a time. This lock prevents conflicts.
        self._lock = asyncio.Lock()

    async def put(
        self, key: str, value: str, labels: dict[str, str]
    ) -> dict[str, Any]:
        async with self._lock:
            timestamp = int(time.time() * 1000)
            obj = {
                "key": key,
                "timestamp": timestamp,
                "value": value,
                "labels": labels,
                "deleted": False,
            }
            self._objects[key] = obj
            await asyncio.to_thread(self._write_to_db, obj)
            return obj

    async def delete(self, key: str) -> dict[str, Any]:
        async with self._lock:
            timestamp = int(time.time() * 1000)
            existing = self._objects.get(key, {})
            obj = {
                "key": key,
                "timestamp": timestamp,
                "value": None,
                "labels": existing.get("labels", {}),
                "deleted": True,
            }
            self._objects[key] = obj
            await asyncio.to_thread(self._write_to_db, obj)
            return obj

    def get(self, key: str) -> dict[str, Any] | None:
        return self._objects.get(key)

    def list_objects(
        self,
        prefix: str | None = None,
        since: int | None = None,
        labels: dict[str, str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        last_key = None
        if cursor:
            last_key = json.loads(base64.b64decode(cursor).decode())["key"]

        limit = min(max(limit, 1), 1000)

        results: list[dict[str, Any]] = []
        for key in sorted(self._objects.keys()):
            if last_key and key <= last_key:
                continue
            obj = self._objects[key]
            if labels:
                obj_labels = obj.get("labels", {})
                match = True
                for name, value in labels.items():
                    if obj_labels.get(name) != value:
                        match = False
                        break
                if not match:
                    continue
            if prefix and not key.startswith(prefix):
                continue
            if since is not None and obj["timestamp"] < since:
                continue
            results.append(obj)
            if len(results) >= limit:
                break

        next_cursor = None
        if results:
            next_cursor = base64.b64encode(
                json.dumps({"key": results[-1]["key"]}).encode()
            ).decode()

        return results, next_cursor

    async def cleanup(self, cutoff_ms: int) -> None:
        """Remove tombstones that are older than cutoff_ms from memory and SQLite."""
        async with self._lock:
            await asyncio.to_thread(self._cleanup_database, cutoff_ms)
            to_remove = [
                key
                for key, obj in self._objects.items()
                if obj["deleted"] and obj["timestamp"] < cutoff_ms
            ]
            for key in to_remove:
                del self._objects[key]

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _load_from_db(self) -> None:
        """Load all objects from the database into the hot-state dictionary."""
        cursor = self._db.execute(
            "SELECT key, timestamp, value, labels, deleted FROM objects"
        )
        for row in cursor:
            key, timestamp, value_json, labels_json, deleted = row
            self._objects[key] = {
                "key": key,
                "timestamp": timestamp,
                "value": value_json if value_json is not None else None,
                "labels": json.loads(labels_json),
                "deleted": bool(deleted),
            }

    def _write_to_db(self, obj: dict[str, Any]) -> None:
        """Write one object to SQLite. Call this from a thread."""
        with self._db:
            self._db.execute(
                """
                INSERT OR REPLACE INTO objects (key, timestamp, value, labels, deleted)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    obj["key"],
                    obj["timestamp"],
                    obj["value"] if obj["value"] is not None else None,
                    json.dumps(obj["labels"]),
                    int(obj["deleted"]),
                ),
            )

    def _cleanup_database(self, cutoff_ms: int) -> None:
        """Delete old tombstones from the SQLite database."""
        with self._db:
            self._db.execute(
                "DELETE FROM objects WHERE deleted = 1 AND timestamp < ?",
                (cutoff_ms,),
            )

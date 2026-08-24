from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from fastapi import WebSocket

_logger = logging.getLogger(__name__)


@dataclass
class Subscription:
    """One subscription from a client to a key or to a set of labels."""
    id: int
    connection_id: int
    key: str | None = None
    labels: dict[str, str] = field(default_factory=dict)


class Watcher:
    """Tracks subscriptions and sends object updates to WebSocket clients."""

    def __init__(self):
        self._next_subscription_id = 1
        # Maps object keys to subscription IDs for fast key lookups.
        self._key_watchers: dict[str, set[int]] = {}
        # Maps label pairs to subscription IDs for fast label lookups.
        self._label_watchers: dict[tuple[str, str], set[int]] = {}
        self._subscriptions: dict[int, Subscription] = {}
        self._connection_subscriptions: dict[int, set[int]] = {}
        self._connection_locks: dict[int, asyncio.Lock] = {}
        self._websockets: dict[int, WebSocket] = {}

    def connect(self, connection_id: int, websocket: WebSocket) -> None:
        """Register a new WebSocket connection."""
        self._websockets[connection_id] = websocket
        self._connection_locks[connection_id] = asyncio.Lock()
        self._connection_subscriptions[connection_id] = set()

    def disconnect(self, connection_id: int) -> None:
        """Remove a connection and all of its subscriptions."""
        for subscription_id in list(self._connection_subscriptions.get(connection_id, set())):
            self._unsubscribe(subscription_id)
        self._connection_subscriptions.pop(connection_id, None)
        self._websockets.pop(connection_id, None)
        self._connection_locks.pop(connection_id, None)

    def subscribe(
        self,
        connection_id: int,
        key: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> int:
        """Add a subscription and return its ID."""
        subscription_id = self._next_subscription_id
        self._next_subscription_id += 1

        sub = Subscription(
            id=subscription_id,
            connection_id=connection_id,
            key=key,
            labels=labels or {},
        )
        self._subscriptions[subscription_id] = sub
        self._connection_subscriptions[connection_id].add(subscription_id)

        if key is not None:
            self._key_watchers.setdefault(key, set()).add(subscription_id)
        for label in sub.labels.items():
            self._label_watchers.setdefault(label, set()).add(subscription_id)

        return subscription_id

    def unsubscribe(self, subscription_id: int) -> None:
        """Remove one subscription."""
        self._unsubscribe(subscription_id)

    async def send_to_connection(self, connection_id: int, obj: dict) -> None:
        """Send one object to a connection. Use a lock so messages do not mix."""
        websocket = self._websockets.get(connection_id)
        lock = self._connection_locks.get(connection_id)
        if websocket is None or lock is None:
            return

        msg = {
            "type": "object",
            "key": obj["key"],
            "timestamp": obj["timestamp"],
            "value": obj["value"],
            "labels": obj["labels"],
            "deleted": obj["deleted"],
        }
        try:
            async with lock:
                await websocket.send_json(msg)
        except Exception:
            _logger.debug(
                "dropped message for connection %s: send failed", connection_id
            )

    async def notify(self, obj: dict) -> None:
        """Find all matching subscriptions and send the object once per connection."""
        matching_subs: set[int] = set()

        # Add subscribers that watch this exact key.
        if obj["key"] in self._key_watchers:
            matching_subs.update(self._key_watchers[obj["key"]])

        # Find label subscriptions that match all of the object's labels.
        obj_labels = frozenset(obj.get("labels", {}).items())
        candidates: set[int] = set()
        for label in obj.get("labels", {}).items():
            candidates.update(self._label_watchers.get(label, set()))

        for subscription_id in candidates:
            sub = self._subscriptions.get(subscription_id)
            if sub is None:
                continue
            required = frozenset(sub.labels.items())
            if required <= obj_labels:
                matching_subs.add(subscription_id)

        # Send only one message per connection, even with multiple matching subscriptions.
        connection_ids = {self._subscriptions[sid].connection_id for sid in matching_subs}

        for connection_id in connection_ids:
            await self.send_to_connection(connection_id, obj)

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _unsubscribe(self, subscription_id: int) -> None:
        sub = self._subscriptions.pop(subscription_id, None)
        if sub is None:
            return

        self._connection_subscriptions[sub.connection_id].discard(subscription_id)
        if sub.key is not None:
            self._key_watchers.get(sub.key, set()).discard(subscription_id)
        for label in sub.labels.items():
            self._label_watchers.get(label, set()).discard(subscription_id)

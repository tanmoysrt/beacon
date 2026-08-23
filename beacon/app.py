import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from beacon.store import Store
from beacon.watcher import Watcher
from beacon.websocket import WebSocketProtocol

# These are set by configure() before the server starts.
store: Store | None = None
manager: Watcher | None = None


def configure(db_path: str) -> None:
    """Create the store and watcher with the given database path."""
    global store, manager
    store = Store(db_path)
    manager = Watcher()
    WebSocketProtocol.handler = EventHandler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start a background task that removes old tombstones every hour."""

    async def _cleanup() -> None:
        while True:
            await asyncio.sleep(3600)
            cutoff = int(time.time() * 1000) - 7 * 24 * 3600 * 1000
            await store.cleanup(cutoff)

    task = asyncio.create_task(_cleanup())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


class PutRequest(BaseModel):
    value: str
    labels: dict[str, str] = {}


class ObjectResponse(BaseModel):
    key: str
    timestamp: int
    value: str | None
    labels: dict[str, str]
    deleted: bool


class ListResponse(BaseModel):
    objects: list[ObjectResponse]
    next_cursor: str | None


class SubscribeMessage(BaseModel):
    type: Literal["subscribe"]
    key: str | None = None
    labels: dict[str, str] | None = None
    since: int | None = None


class UnsubscribeMessage(BaseModel):
    type: Literal["unsubscribe"]
    subscription_id: int


WebsocketMessage = Annotated[
    SubscribeMessage | UnsubscribeMessage,
    Field(discriminator="type"),
]
_websocket_adapter = TypeAdapter(WebsocketMessage)


@app.get("/objects", response_model=ListResponse)
async def list_objects(
    request: Request,
    prefix: Annotated[str | None, Query()] = None,
    since: Annotated[int | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    cursor: Annotated[str | None, Query()] = None,
):
    labels = {
        name.removeprefix("label."): value
        for name, value in request.query_params.multi_items()
        if name.startswith("label.")
    }
    objects, next_cursor = store.list_objects(
        prefix=prefix, since=since, labels=labels, limit=limit, cursor=cursor
    )
    return {"objects": objects, "next_cursor": next_cursor}


@app.put("/objects/{key:path}", response_model=ObjectResponse)
async def put_object(key: str, request: PutRequest):
    obj = await store.put(key, request.value, request.labels)
    manager.notify(obj)
    return obj


@app.get("/objects/{key:path}", response_model=ObjectResponse)
async def get_object(key: str):
    obj = store.get(key)
    if obj is None:
        raise HTTPException(status_code=404, detail="not found")
    return obj


@app.delete("/objects/{key:path}", response_model=ObjectResponse)
async def delete_object(key: str):
    obj = await store.delete(key)
    manager.notify(obj)
    return obj


def _send_catch_up(connection_id: int, msg: SubscribeMessage) -> None:
    """Send all objects that changed after msg.since to a new subscriber."""
    if msg.since is None:
        return
    if msg.key is not None:
        obj = store.get(msg.key)
        if obj and obj["timestamp"] >= msg.since:
            manager.send_to_connection(connection_id, obj)
        return
    objects, _ = store.list_objects(labels=msg.labels, since=msg.since, limit=100_000)
    for obj in objects:
        manager.send_to_connection(connection_id, obj)


class EventHandler:
    """Handles the WebSocket connections that uvicorn passes to the protocol."""

    def on_connect(self, connection_id: int, connection: WebSocketProtocol) -> None:
        manager.connect(connection_id, connection)

    def on_disconnect(self, connection_id: int) -> None:
        manager.disconnect(connection_id)

    def on_message(
        self, connection_id: int, connection: WebSocketProtocol, raw: str
    ) -> None:
        try:
            data = json.loads(raw)
            msg = _websocket_adapter.validate_python(data)
        except (ValueError, ValidationError) as exc:
            connection.send(
                json.dumps(
                    {"type": "error", "message": f"validation error: {exc}", "data": raw}
                )
            )
            return

        if isinstance(msg, SubscribeMessage):
            subscription_id = manager.subscribe(
                connection_id, key=msg.key, labels=msg.labels
            )
            connection.send(
                json.dumps(
                    {"type": "subscribed", "subscription_id": subscription_id}
                )
            )
            _send_catch_up(connection_id, msg)
            return

        manager.unsubscribe(msg.subscription_id)
        connection.send(
            json.dumps(
                {"type": "unsubscribed", "subscription_id": msg.subscription_id}
            )
        )

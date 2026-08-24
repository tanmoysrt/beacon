import asyncio
import itertools
import json
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import Annotated, Literal

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from beacon.store import Store
from beacon.watcher import Watcher

# These are set by configure() before the server starts.
store: Store | None = None
manager: Watcher | None = None

CLEANUP_INTERVAL_S = 3600
TOMBSTONE_TTL_MS = 7 * 24 * 3600 * 1000
CATCH_UP_LIMIT = 100_000

connection_ids = itertools.count(1)


def configure(db_path: str) -> None:
    """Create the store and watcher with the given database path."""
    global store, manager
    store = Store(db_path)
    manager = Watcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start a background task that removes old tombstones every hour."""

    async def _cleanup() -> None:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_S)
            try:
                cutoff = int(time.time() * 1000) - TOMBSTONE_TTL_MS
                await store.cleanup(cutoff)
            except Exception:
                logging.getLogger(__name__).exception("tombstone cleanup failed")

    task = asyncio.create_task(_cleanup())
    yield
    task.cancel()

    with suppress(asyncio.CancelledError):
        await task


app = FastAPI(lifespan=lifespan)


class PutRequest(BaseModel):
    value: str
    labels: dict[str, str] = Field(default_factory=dict)


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

    try:
        objects, next_cursor = store.list_objects(
            prefix=prefix, since=since, labels=labels, limit=limit, cursor=cursor
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="invalid cursor")

    return {"objects": objects, "next_cursor": next_cursor}


@app.put("/objects/{key:path}", response_model=ObjectResponse)
async def put_object(key: str, request: PutRequest):
    obj = await store.put(key, request.value, request.labels)
    await manager.notify(obj)
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
    await manager.notify(obj)
    return obj


async def _send_catch_up(connection_id: int, msg: SubscribeMessage) -> None:
    """Send all objects that changed after msg.since to a new subscriber."""
    if msg.since is None:
        return

    if msg.key is not None:
        obj = store.get(msg.key)
        if obj and obj["timestamp"] >= msg.since:
            await manager.send_to_connection(connection_id, obj)
        return

    objects, _ = store.list_objects(
        labels=msg.labels, since=msg.since, limit=CATCH_UP_LIMIT
    )

    for obj in objects:
        await manager.send_to_connection(connection_id, obj)


async def _handle_subscribe_message(
    websocket: WebSocket, connection_id: int, msg: SubscribeMessage
) -> None:
    subscription_id = manager.subscribe(connection_id, key=msg.key, labels=msg.labels)
    await websocket.send_json(
        {"type": "subscribed", "subscription_id": subscription_id}
    )
    await _send_catch_up(connection_id, msg)


async def _handle_unsubscribe_message(
    websocket: WebSocket, msg: UnsubscribeMessage
) -> None:
    manager.unsubscribe(msg.subscription_id)
    await websocket.send_json(
        {"type": "unsubscribed", "subscription_id": msg.subscription_id}
    )


async def _dispatch_websocket_message(
    websocket: WebSocket, connection_id: int, msg: WebsocketMessage
) -> None:
    if isinstance(msg, SubscribeMessage):
        await _handle_subscribe_message(websocket, connection_id, msg)
    elif isinstance(msg, UnsubscribeMessage):
        await _handle_unsubscribe_message(websocket, msg)


async def _handle_events(websocket: WebSocket, connection_id: int) -> None:
    while True:
        try:
            raw = await websocket.receive_json()
        except json.JSONDecodeError as exc:
            await websocket.send_json(
                {"type": "error", "message": f"invalid JSON: {exc}", "data": None}
            )
            continue
        try:
            msg = _websocket_adapter.validate_python(raw)
        except ValidationError as exc:
            await websocket.send_json(
                {
                    "type": "error",
                    "message": f"validation error: {exc}",
                    "data": raw,
                }
            )
            continue
        await _dispatch_websocket_message(websocket, connection_id, msg)


@app.websocket("/events")
async def handle_websocket(websocket: WebSocket):
    """Accept a WebSocket connection and process event messages until it closes."""
    await websocket.accept()
    connection_id = next(connection_ids)
    manager.connect(connection_id, websocket)

    try:
        await _handle_events(websocket, connection_id)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(connection_id)

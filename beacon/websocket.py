from __future__ import annotations

import asyncio
from typing import Any, Protocol as TypingProtocol

from websockets.frames import Opcode
from websockets.http11 import Request
from websockets.server import ServerProtocol

PATH = "/events"


class Handler(TypingProtocol):
    """What the protocol needs from the application."""

    def on_connect(self, connection_id: int, connection: WebSocketProtocol) -> None: ...

    def on_message(
        self, connection_id: int, connection: WebSocketProtocol, raw: str
    ) -> None: ...

    def on_disconnect(self, connection_id: int) -> None: ...


class WebSocketProtocol(asyncio.Protocol):
    """Serves WebSocket connections without the ASGI layer.

    Uvicorn serves all HTTP and keeps the port. When a request asks to upgrade,
    uvicorn hands the transport to this class. The class then reads and writes
    frames directly, so a notification costs no ASGI message.
    """

    handler: Handler | None = None

    def __init__(
        self,
        config: Any,
        server_state: Any,
        app_state: dict[str, Any],
        _loop: asyncio.AbstractEventLoop | None = None,
    ):
        self._connection = ServerProtocol()
        self._connections = server_state.connections
        self._transport: asyncio.Transport | None = None
        self._accepted = False
        self.connection_id = id(self)

    def send(self, message: str) -> None:
        """Send one text message. Frames go out in the order of the calls."""
        if not self._accepted or self._transport is None or self._transport.is_closing():
            return
        self._connection.send_text(message.encode())
        self._flush()

    def buffered_bytes(self) -> int:
        if self._transport is None:
            return 0
        return self._transport.get_write_buffer_size()

    # ------------------------------------------------------------------
    # asyncio.Protocol
    # ------------------------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport
        self._connections.add(self)

    def connection_lost(self, exc: Exception | None) -> None:
        self._connections.discard(self)
        if self._accepted and self.handler is not None:
            self.handler.on_disconnect(self.connection_id)
            self._accepted = False

    def data_received(self, data: bytes) -> None:
        self._connection.receive_data(data)
        for event in self._connection.events_received():
            if isinstance(event, Request):
                self._handshake(event)
            elif event.opcode is Opcode.TEXT and self.handler is not None:
                self.handler.on_message(
                    self.connection_id, self, event.data.decode()
                )
        self._flush()

    def eof_received(self) -> None:
        return None

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _handshake(self, request: Request) -> None:
        if request.path.split("?")[0] != PATH:
            self._connection.send_response(
                self._connection.reject(404, "not found")
            )
            return
        self._connection.send_response(self._connection.accept(request))
        self._accepted = True
        if self.handler is not None:
            self.handler.on_connect(self.connection_id, self)

    def _flush(self) -> None:
        if self._transport is None:
            return
        for data in self._connection.data_to_send():
            if data:
                self._transport.write(data)
            elif not self._transport.is_closing():
                self._transport.close()

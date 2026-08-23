import json
from types import SimpleNamespace

from websockets.client import ClientProtocol
from websockets.uri import parse_uri

from beacon.app import configure
from beacon.websocket import WebSocketProtocol


class FakeTransport:
    def __init__(self):
        self.data = bytearray()
        self.closing = False

    def write(self, data):
        self.data += data

    def close(self):
        self.closing = True

    def is_closing(self):
        return self.closing

    def get_write_buffer_size(self):
        return 0


class Client:
    """Drives a WebSocketProtocol through a real handshake and real frames."""

    def __init__(self, path="/events"):
        self.transport = FakeTransport()
        server_state = SimpleNamespace(connections=set(), tasks=set())
        self.protocol = WebSocketProtocol(
            config=None, server_state=server_state, app_state={}
        )
        self.protocol.connection_made(self.transport)
        self.client = ClientProtocol(parse_uri(f"ws://test{path}"))
        self.client.send_request(self.client.connect())
        self._push()
        self.handshake = self._pull()

    @property
    def accepted(self):
        return self.client.state.name == "OPEN"

    def send(self, message):
        self.client.send_text(json.dumps(message).encode())
        self._push()

    def received(self):
        messages = []
        for event in self._pull():
            data = getattr(event, "data", None)
            if isinstance(data, (bytes, bytearray)):
                messages.append(json.loads(data))
        return messages

    def _push(self):
        for data in self.client.data_to_send():
            if data:
                self.protocol.data_received(data)

    def _pull(self):
        data = bytes(self.transport.data)
        self.transport.data.clear()
        if data:
            self.client.receive_data(data)
        return self.client.events_received()


class TestWebSocketProtocol:
    def test_handshake_accepts_the_events_path(self):
        configure(":memory:")
        client = Client()
        assert client.accepted

    def test_handshake_rejects_another_path(self):
        configure(":memory:")
        client = Client(path="/wrong")
        assert not client.accepted

    def test_subscribe_then_receive_update(self):
        configure(":memory:")
        import beacon.app as app

        client = Client()
        client.send({"type": "subscribe", "key": "k1"})
        assert client.received()[0]["type"] == "subscribed"

        app.manager.notify(
            {
                "key": "k1",
                "timestamp": 1,
                "value": "v1",
                "labels": {},
                "deleted": False,
            }
        )
        message = client.received()[0]
        assert message["type"] == "object"
        assert message["key"] == "k1"
        assert message["value"] == "v1"

    def test_unsubscribe(self):
        configure(":memory:")
        client = Client()
        client.send({"type": "subscribe", "key": "k1"})
        subscription_id = client.received()[0]["subscription_id"]

        client.send({"type": "unsubscribe", "subscription_id": subscription_id})
        assert client.received()[0]["type"] == "unsubscribed"

    def test_invalid_message_returns_error(self):
        configure(":memory:")
        client = Client()
        client.send({"type": "unknown"})
        assert client.received()[0]["type"] == "error"

    def test_malformed_json_returns_error(self):
        configure(":memory:")
        client = Client()
        client.client.send_text(b"{not json")
        client._push()
        assert client.received()[0]["type"] == "error"

    def test_disconnect_removes_the_connection(self):
        configure(":memory:")
        import beacon.app as app

        client = Client()
        client.send({"type": "subscribe", "key": "k1"})
        client.received()
        client.protocol.connection_lost(None)

        assert len(app.manager._connections) == 0
        assert len(app.manager._subscriptions) == 0

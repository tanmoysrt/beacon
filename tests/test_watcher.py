import json

from beacon.watcher import Watcher


class FakeConnection:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(json.loads(message))

    def buffered_bytes(self):
        return 0


class TestWatcher:
    def test_key_subscription_matches(self):
        watcher = Watcher()
        connection = FakeConnection()
        watcher.connect(1, connection)
        watcher.subscribe(1, key="k1")

        watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {}, "deleted": False})

        assert [m["key"] for m in connection.sent] == ["k1"]

    def test_label_subscription_matches_all_labels(self):
        watcher = Watcher()
        connection = FakeConnection()
        watcher.connect(1, connection)
        watcher.subscribe(1, labels={"env": "prod", "tier": "web"})

        watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {"env": "prod", "tier": "web", "zone": "us"}, "deleted": False})

        assert len(connection.sent) == 1

    def test_label_subscription_ignores_a_partial_match(self):
        watcher = Watcher()
        connection = FakeConnection()
        watcher.connect(1, connection)
        watcher.subscribe(1, labels={"env": "prod", "tier": "web"})

        watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {"env": "prod"}, "deleted": False})

        assert connection.sent == []

    def test_notify_deduplicates_per_connection(self):
        watcher = Watcher()
        connection = FakeConnection()
        watcher.connect(1, connection)
        watcher.subscribe(1, key="k1")
        watcher.subscribe(1, labels={"env": "prod"})

        watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {"env": "prod"}, "deleted": False})

        assert len(connection.sent) == 1

    def test_notify_reaches_every_matching_connection(self):
        watcher = Watcher()
        connections = {}
        for connection_id in (1, 2, 3):
            connections[connection_id] = FakeConnection()
            watcher.connect(connection_id, connections[connection_id])
            watcher.subscribe(connection_id, key="k1")

        watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {}, "deleted": False})

        assert all(len(c.sent) == 1 for c in connections.values())

    def test_disconnect_removes_subscriptions(self):
        watcher = Watcher()
        watcher.connect(1, FakeConnection())
        watcher.subscribe(1, key="k1")
        watcher.disconnect(1)

        assert len(watcher._subscriptions) == 0
        assert len(watcher._key_watchers.get("k1", set())) == 0

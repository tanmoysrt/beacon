import asyncio

from beacon.watcher import Watcher


class TestWatcher:
    def test_key_subscription_matches(self):
        watcher = Watcher()
        watcher.connect(1, None)
        watcher.subscribe(1, key="k1")

        notified = []
        async def capture(cid, obj):
            notified.append((cid, obj["key"]))

        watcher.send_to_connection = capture
        asyncio.run(watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {}, "deleted": False}))

        assert notified == [(1, "k1")]

    def test_label_subscription_matches_all_labels(self):
        watcher = Watcher()
        watcher.connect(1, None)
        watcher.subscribe(1, labels={"env": "prod", "tier": "web"})

        notified = []
        async def capture(cid, obj):
            notified.append(cid)

        watcher.send_to_connection = capture
        asyncio.run(watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {"env": "prod", "tier": "web", "zone": "us"}, "deleted": False}))

        assert notified == [1]

    def test_notify_deduplicates_per_connection(self):
        watcher = Watcher()
        watcher.connect(1, None)
        watcher.subscribe(1, key="k1")
        watcher.subscribe(1, labels={"env": "prod"})

        notified = []
        async def capture(cid, obj):
            notified.append(cid)

        watcher.send_to_connection = capture
        asyncio.run(watcher.notify({"key": "k1", "timestamp": 1, "value": "v", "labels": {"env": "prod"}, "deleted": False}))

        assert notified == [1]

    def test_disconnect_removes_subscriptions(self):
        watcher = Watcher()
        watcher.connect(1, None)
        watcher.subscribe(1, key="k1")
        watcher.disconnect(1)

        assert len(watcher._subscriptions) == 0
        assert len(watcher._key_watchers.get("k1", set())) == 0

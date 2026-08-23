import asyncio

import pytest

from beacon.store import Store


@pytest.fixture
def store(tmp_path):
    db = tmp_path / "test.db"
    return Store(str(db))


class TestStore:
    def test_put_and_get(self, store):
        obj = asyncio.run(store.put("k1", "v1", {"env": "prod"}))
        assert obj["key"] == "k1"
        assert obj["value"] == "v1"
        assert obj["labels"] == {"env": "prod"}
        assert obj["deleted"] is False

        got = store.get("k1")
        assert got["value"] == "v1"

    def test_delete_makes_tombstone(self, store):
        asyncio.run(store.put("k1", "v1", {}))
        obj = asyncio.run(store.delete("k1"))
        assert obj["deleted"] is True
        assert obj["value"] is None

        got = store.get("k1")
        assert got["deleted"] is True

    def test_list_filters_labels_first(self, store):
        asyncio.run(store.put("a", "1", {"env": "prod"}))
        asyncio.run(store.put("b", "2", {"env": "dev"}))
        asyncio.run(store.put("c", "3", {"env": "prod"}))

        results, _ = store.list_objects(labels={"env": "prod"})
        assert [r["key"] for r in results] == ["a", "c"]

    def test_list_prefix_and_labels(self, store):
        asyncio.run(store.put("alpha", "1", {"env": "prod"}))
        asyncio.run(store.put("beta", "2", {"env": "prod"}))

        results, _ = store.list_objects(prefix="a", labels={"env": "prod"})
        assert [r["key"] for r in results] == ["alpha"]

    def test_pagination_cursor(self, store):
        asyncio.run(store.put("a", "1", {}))
        asyncio.run(store.put("b", "2", {}))
        asyncio.run(store.put("c", "3", {}))

        page1, cursor = store.list_objects(limit=2)
        assert [r["key"] for r in page1] == ["a", "b"]
        assert cursor is not None

        page2, _ = store.list_objects(limit=2, cursor=cursor)
        assert [r["key"] for r in page2] == ["c"]

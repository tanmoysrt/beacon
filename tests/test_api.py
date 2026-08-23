from fastapi.testclient import TestClient

from beacon.app import app, configure


class TestHttpApi:
    def test_put_and_get(self):
        configure(":memory:")
        client = TestClient(app)
        r = client.put("/objects/k1", json={"value": "v1", "labels": {"env": "prod"}})
        assert r.status_code == 200
        assert r.json()["value"] == "v1"

        r = client.get("/objects/k1")
        assert r.status_code == 200
        assert r.json()["value"] == "v1"

    def test_get_missing_returns_404(self):
        configure(":memory:")
        client = TestClient(app)
        r = client.get("/objects/missing")
        assert r.status_code == 404

    def test_delete_makes_tombstone(self):
        configure(":memory:")
        client = TestClient(app)
        client.put("/objects/k1", json={"value": "v1", "labels": {}})
        r = client.delete("/objects/k1")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    def test_list_with_filters(self):
        configure(":memory:")
        client = TestClient(app)
        client.put("/objects/a", json={"value": "1", "labels": {"env": "prod"}})
        client.put("/objects/b", json={"value": "2", "labels": {"env": "dev"}})

        r = client.get("/objects?label.env=prod")
        assert r.status_code == 200
        assert len(r.json()["objects"]) == 1
        assert r.json()["objects"][0]["key"] == "a"

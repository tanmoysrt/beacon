# Beacon

Beacon is a small coordination service.

Beacon keeps key-value objects and tells clients when the objects change. It does not know what an object means. It gives you storage, labels, timestamps, and change notifications. Your application gives the meaning.

## Install

Beacon needs Python 3.14 or later.

```bash
# PIP
pip install git+https://github.com/tanmoysrt/beacon.git

# UV
uv tool install git+https://github.com/tanmoysrt/beacon.git

# PIPX
pipx install git+https://github.com/tanmoysrt/beacon.git
```

## Run

```bash
beacon --host 127.0.0.1 --port 8000 --database beacon.db
```

## Quick start

Write an object:

```bash
curl -X PUT http://127.0.0.1:8000/objects/server/1 \
  -H 'Content-Type: application/json' \
  -d '{"value": "ready", "labels": {"group": "web", "region": "india"}}'
```

Read it:

```bash
curl http://127.0.0.1:8000/objects/server/1
```

Find all objects with the same two labels:

```bash
curl 'http://127.0.0.1:8000/objects?label.group=web&label.region=india'
```

Watch every object in the group. Beacon sends one message for each change:

```python
import json
from websockets.sync.client import connect

with connect("ws://127.0.0.1:8000/events") as ws:
    ws.send(json.dumps({"type": "subscribe", "labels": {"group": "web"}}))
    while True:
        print(json.loads(ws.recv()))
```

## Documentation

- [API Reference](docs/api.md) - the HTTP and WebSocket messages.
- [Architecture](docs/architecture.md) - the paths through the process, and the guarantees.

## License

MIT. Read the [LICENSE](LICENSE) file.

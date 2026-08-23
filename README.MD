# Beacon

Beacon is a small coordination service.

Beacon keeps key-value objects and tells clients when the objects change. It does not know what an object means. It gives you storage, labels, timestamps, and change notifications. Your application gives the meaning.

## Data model

An object has five fields and nothing more:

| Field       | Type           | Description                                               |
| ----------- | -------------- | --------------------------------------------------------- |
| `key`       | string         | The identifier. Slashes are allowed.                      |
| `timestamp` | integer        | The time of the last change, in Unix milliseconds.        |
| `value`     | string or null | A value that Beacon does not read.                        |
| `labels`    | object         | String-to-string pairs. Clients select objects with them. |
| `deleted`   | boolean        | `true` for a tombstone.                                   |

## How Beacon works

Beacon runs as one process:

- The **store** keeps all current objects in memory, and writes each change to SQLite.
- The **watcher** keeps an index of subscriptions, and sends each change to the clients that watch it.

At startup, Beacon reads all objects from SQLite into memory.

## Install

```bash
pip install -e .
```

Beacon needs Python 3.14 or later.

## Run

```bash
beacon --host 127.0.0.1 --port 8000 --database beacon.db
```

All three options have defaults. The values above are the defaults.

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

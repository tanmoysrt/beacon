# Architecture

Beacon is one Python process with two parts: a **store** and a **watcher**. There is no cluster, no broker, and no worker process.

```mermaid
flowchart TB
    HTTP["HTTP endpoints: FastAPI on uvicorn"] --> MEM
    HTTP -->|"after each write"| IDX
    WS["WebSocket /events: own protocol, no ASGI"] -->|"subscribe"| IDX
    CLEAN["Cleanup task: every hour"] --> MEM

    subgraph S["Store"]
        MEM["Memory: all current objects"] --> DB[("SQLite: WAL mode")]
    end

    subgraph W["Watcher"]
        IDX["Key index and label index"] --> CONN["Open connections"]
    end
```

## Store

The store keeps two copies of the same current state:

| Copy | Purpose |
| --- | --- |
| Memory (a Python dictionary) | Fast reads, fast lists, and fast label matching. |
| SQLite (WAL mode) | Keeps the data safe across a restart. |

Beacon reads all objects from SQLite into memory at startup. After that, every read comes from memory. SQLite receives writes only.

The store has one write lock. SQLite processes one write at a time, so the lock keeps the two copies in the same order.

### Write path

```mermaid
flowchart TB
    A["PUT or DELETE arrives"] --> B["Take the write lock"]
    B --> C["Make the object with a new timestamp"]
    C --> D["Put the object in memory"]
    D --> E{"Write the object to SQLite"}
    E -->|"the write is good"| F["Release the write lock"]
    F --> G["Send the object to the matching connections"]
    G --> H["Return the object in the response"]
    E -->|"the write fails"| X["Return an error. Send no message"]
```

Beacon changes memory **before** it writes to SQLite. If the SQLite write fails, three things happen:

1. The request returns an error.
2. Beacon sends no notification.
3. Memory holds the new object, but SQLite does not.

The two copies then differ until the next restart. A restart reads SQLite, so the failed write disappears.

### Read path

A read does not use SQLite:

```mermaid
flowchart LR
    A["GET one object"] --> B["Memory lookup"]
    B --> C["The object, or 404"]
    D["GET a list"] --> E["Sort all keys"]
    E --> F["Filter: cursor, labels, prefix, since"]
    F --> G["Stop at the limit"]
    G --> H["The page and a cursor"]
```

`GET /objects` sorts and scans all keys in memory. The cost grows with the total number of objects, not with the size of the page.

## Watcher

One connection can hold many subscriptions. A subscription selects an exact key, or a set of labels.

The watcher keeps two indexes. Both point to subscription identifiers:

```mermaid
flowchart LR
    K["key"] --> KS["set of subscription IDs"]
    L["label name and label value"] --> LS["set of subscription IDs"]
```

The indexes prevent a scan of all connections for each change. This is necessary at approximately 10,000 connections.

### Match path

```mermaid
flowchart TB
    A["An object changes"] --> B["Get the subscriptions for the object key"]
    A --> C["Get the subscriptions for each label of the object"]
    C --> D{"Are all labels of the subscription on the object?"}
    D -- "yes" --> E["The subscription matches"]
    D -- "no" --> F["Skip the subscription"]
    B --> G["Collect the connections of the matching subscriptions"]
    E --> G
    G --> I["Encode the message one time"]
    I --> H["Write it to the socket of each connection"]
```

Multiple labels always mean **AND**. There is no OR, no NOT, and no query language.

A connection receives the object one time only, even if many of its subscriptions match.

`notify` encodes the message one time for all connections, then writes it to each socket. It does not wait for a socket, so a write costs the same with one watcher or ten thousand.

Writes reach a socket in the order of the calls, so the messages of one connection stay in order. A direct write has no backpressure, so the watcher checks what the socket still holds and drops the message above 1 MB. If a send fails, Beacon does not report it.

### Why /events is not an ASGI endpoint

Uvicorn serves all HTTP and owns the port. When a request asks to upgrade, uvicorn passes the socket to `beacon.websocket.WebSocketProtocol`, which reads and writes frames itself.

An ASGI WebSocket costs one message dictionary and one protocol round trip for each notification, and that cost is larger than the write. Measured on one core with 500 subscribers on one key:

| Path | CPU for each notification | Notifications each second |
| --- | --- | --- |
| ASGI WebSocket endpoint | 22.3 us | 44,900 |
| Own protocol | 7.9 us | 126,000 |

The HTTP API keeps FastAPI, its validation, and its response models. Only `/events` leaves ASGI.

## Object lifecycle

```mermaid
stateDiagram-v2
    [*] --> Live: PUT
    Live --> Live: PUT replaces the value and the labels
    Live --> Tombstone: DELETE
    Tombstone --> Live: PUT
    Tombstone --> [*]: cleanup after 7 days
```

A `DELETE` does not remove the row. It makes a **tombstone**: `deleted` is `true` and `value` is `null`. The tombstone keeps the labels of the object, so the label watchers also receive the deletion.

A tombstone is a normal object. `GET /objects/{key}` and `GET /objects` both return it.

A background task starts one hour after startup, and then runs every hour. It removes each tombstone that is older than seven days from SQLite and from memory. A key is a `404` again after the cleanup.

## Recovery

Beacon keeps no event log. A client that misses a change reads the current state instead:

```mermaid
sequenceDiagram
    participant C as Client
    participant B as Beacon
    C->>B: GET /objects with since=T
    B-->>C: the objects that changed at T or after T
    C->>B: subscribe again on the WebSocket
    B-->>C: subscribed
    B-->>C: object messages
```

A client keeps the highest timestamp that it processed. A `subscribe` message can also include `since`. Beacon then registers the subscription first, and sends the changed objects after it.

## Guarantees

| Property | Behavior |
| --- | --- |
| Durable data | After a good write, the object is in SQLite. |
| Delivery | Best effort only. Beacon does not send a failed message again. |
| Duplicates | A client can receive the same object more than once. |
| Order | Messages to one connection stay in order. |
| Slow consumers | Beacon drops messages for a connection that holds more than 1 MB in its socket. |
| Timestamps | Unix milliseconds. Two changes can share one timestamp. |
| Conflicts | The last write wins. |

## Design rules

Beacon does not have these features:

- authentication and authorization
- compare-and-swap, locks, and leases
- replication, consensus, and leader election
- an event log and event replay
- OR, NOT, and range queries

Add a feature only when a real requirement needs it. Read [AGENTS.MD](../AGENTS.MD) before you make a change.

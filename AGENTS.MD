# Beacon - Agent Instructions

**Core rule: Keep Beacon simple. Simplicity stays long.** Implement only current requirements. Do not solve hypothetical future problems. When a real problem appears, add the smallest fix.

## Architecture
Use Python, FastAPI, Uvicorn, SQLite (`sqlite3`), and native WebSocket. Run one process with one SQLite database and in-memory state.

Do not add Redis, PostgreSQL, ORM layers, message brokers, Raft, gossip, consensus, replication, leader election, distributed locks, compare-and-swap, event sourcing, event replay, or worker systems unless a real requirement demands them.

## Generic design
Beacon knows only:
```text
key, timestamp, value, labels, deleted
```
It must not know about machines, networks, virtual private clouds, snapshots, or other domain objects.

HTTP: `PUT /objects/{key}`, `GET /objects/{key}`, `DELETE /objects/{key}`, `GET /objects`. Listing supports cursor pagination, prefix, timestamp, and labels. Labels are string-to-string pairs; multiple labels always mean **AND**. Never implement OR, NOT, or a query language. Deleted objects are tombstones for seven days.

## WebSocket
One connection can have multiple exact-key or multi-label subscriptions. A subscription to a nonexistent key is valid. Use timestamps for recovery. At-least-once and duplicate delivery are acceptable. Do not build an event log or replay system.

For about 10,000 connections, never scan every connection for each update. Use:
```text
key -> set of connection identifiers
(label name, label value) -> set of connection identifiers
```
For multi-label matching, intersect candidate sets and start with the smallest set. Use Python `dict`, `set`, `tuple`, and `frozenset`. Do not build a custom indexing system.

## Code quality
- Follow **ASD-STE100 Simplified Technical English** in comments and documentation.
- Keep names clear; avoid unnecessary abbreviations.
- Follow sensible object-oriented design. Do not create classes without clear responsibility.
- Keep functions **under 50 lines**; prefer 25 or fewer when practical.
- Keep cyclomatic complexity **below 8**.
- Prefer simple control flow and early returns.
- Avoid deep inheritance, unnecessary interfaces, design patterns, and abstraction layers.
- A small amount of duplication is better than unnecessary abstraction.
- Prefer the standard library and existing dependencies.
- Do not add comments for obvious code or changes. Make the code self-explanatory.
- Add comments only when the reason or behavior is not obvious to a reader.
- Comments should explain why the code uses a non-obvious approach, not what the code does.
- Never add comments at the top of a file.

## Documentation

Check the `docs/` folder and `README.md` for the current API, architecture, and behavior before making changes. Update docs when behavior changes.

## Development
Before adding code, ask: Is it required now? Is there a simpler solution? Can existing data structures solve it? Does it add a dependency, process, abstraction, or subsystem?

If it is only "useful later", do not add it. Keep changes small and focused. Do not refactor working code without a current reason. Test real behavior.

**When choosing between a simple design and a sophisticated design, choose the simple design.**

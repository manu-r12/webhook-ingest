# SOLUTION.md

## What was broken and why

I started from the ops report and traced each symptom back to the code.

**Duplicate records + stats drifting**
The `events` table had a regular index on `event_id`, not a unique one -
so the database never actually prevented duplicate rows. The code checked
if an event existed before inserting, but two concurrent requests could
both pass that check before either inserted. Both would insert. Fixed by
adding a UNIQUE constraint and using `ON CONFLICT DO NOTHING` in the
insert, so the database itself rejects duplicates atomically.

<img width="1332" height="565" alt="Screenshot 2026-08-19 at 2 17 48 PM" src="https://github.com/user-attachments/assets/b237298c-ae2d-4481-bf36-ac7b8987a075" />


Fixed  by adding a UNIQUE constraint and using `ON CONFLICT DO NOTHING` —
the database itself rejects duplicates atomically, no race possible.

<img width="1208" height="581" alt="Screenshot 2026-08-19 at 2 25 39 PM" src="https://github.com/user-attachments/assets/af7b74e8-9dd4-41a2-9e1e-b4e9c7d70022" />





---

## Tracked issues and pull requests

I filed a GitHub issue for each bug before fixing it, then opened a PR per fix so the history is easy to follow.

| Issue | PR | What it fixed |
|---|---|---|
| [#1 Duplicate records and stats drifting](https://github.com/manu-r12/webhook-ingest/issues/1) | [#PR](link) | UNIQUE constraint + `ON CONFLICT DO NOTHING` + `IngestTx` transaction |
| [#2 Recordings never marked processed](https://github.com/manu-r12/webhook-ingest/issues/2) | [#PR](link) | `context.WithoutCancel` + error logging |
| [#4 In-flight tasks lost on shutdown](https://github.com/manu-r12/webhook-ingest/issues/4) | [#PR](link) | `sync.WaitGroup` drain on `Close()` |
| [#6 Stats return zero after restart](https://github.com/manu-r12/webhook-ingest/issues/6) | [#PR](link) | Postgres fallback on cache miss |

---

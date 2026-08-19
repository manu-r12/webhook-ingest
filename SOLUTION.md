# SOLUTION.md

## What was broken and why

I started from the ops report and traced each symptom back to the code.

### 1. Duplicate records + stats drifting
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


### 2. Recordings never marked as processed
The background goroutine used `r.Context()` - the HTTP request context.
The moment the handler sent back a 200, that context was cancelled, so
`MarkRecordingProcessed` always failed silently. The error was also
swallowed with a `// TODO: handle` comment, nothing ever appeared in
the logs. Fixed by switching to `context.WithoutCancel(ctx)` and
actually logging the error.

<img width="701" height="431" alt="Screenshot 2026-08-19 at 2 29 33 PM" src="https://github.com/user-attachments/assets/5cc2527a-094d-4c23-a5af-2630cdb3e22a" />


Fixed by switching to `context.WithoutCancel(ctx)` -> this creates a new 
context that keeps all the values from the parent (like tracing IDs) but 
strips the cancellation signal, so the goroutine stays alive even after 
the HTTP handler returns. Errors are now logged with the call ID so 
failures are visible in the logs.

<img width="732" height="427" alt="Screenshot 2026-08-19 at 2 30 53 PM" src="https://github.com/user-attachments/assets/804d6fe5-86aa-4cbc-af77-5c47b1aa4a16" />

---

### 3. In-flight tasks lost on deployment/shutdown
* **What was broken:** Background recording tasks ran in detached, unmanaged goroutines (`go func()`). During deployment shutdowns, `srv.Shutdown()` waited for active HTTP handlers but did not wait for background tasks. `main()` exited immediately and closed PostgreSQL pools (`st.Close()`), killing in-flight tasks mid-flight.
* **How it was fixed:** Tracked background tasks using a `sync.WaitGroup` in `ingest.Service` and exposed `svc.Close()` (`s.wg.Wait()`), calling it during graceful shutdown in `main.go` before database pools close.

## Tracked issues and pull requests

I filed a GitHub issue for each bug before fixing it, then opened a PR per fix so the history is easy to follow.

| Issue | PR | What it fixed |
|---|---|---|
| [#1 Duplicate records and stats drifting](https://github.com/manu-r12/webhook-ingest/issues/1) | [#PR](link) | UNIQUE constraint + `ON CONFLICT DO NOTHING` + `IngestTx` transaction |
| [#2 Recordings never marked processed](https://github.com/manu-r12/webhook-ingest/issues/2) | [#PR](link) | `context.WithoutCancel` + error logging |
| [#4 In-flight tasks lost on shutdown](https://github.com/manu-r12/webhook-ingest/issues/4) | [#PR](link) | `sync.WaitGroup` drain on `Close()` |
| [#6 Stats return zero after restart](https://github.com/manu-r12/webhook-ingest/issues/6) | [#PR](link) | Postgres fallback on cache miss |

---

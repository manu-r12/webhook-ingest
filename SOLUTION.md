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
Background recording goroutines were unmanaged - when the server received
SIGTERM, `main()` shut down the HTTP server and immediately closed the
Postgres pool. Any goroutine still sleeping through its 50ms of work
woke up to a closed connection pool and failed silently.

<img width="701" height="431" alt="Screenshot 2026-08-19 at 2 29 33 PM" src="https://github.com/user-attachments/assets/214b0a92-7d0d-44ba-9328-774b6b422070" />


Fixed by adding a `sync.WaitGroup` to the service. Every goroutine calls
`wg.Add(1)` on launch and `wg.Done()` when it finishes. `svc.Close()`
calls `wg.Wait()` which blocks until every in-flight goroutine completes.
In `main.go`, `svc.Close()` is called before `st.Close()` - so the pool
stays open until all work is done.

<img width="732" height="427" alt="Screenshot 2026-08-19 at 2 30 53 PM" src="https://github.com/user-attachments/assets/707b0ed8-8ff2-4ac1-a300-3fda3a63e3dd" />

### 4. Stats return zero after service restart
The `GET /accounts/{account_id}/stats` endpoint read exclusively from the in-memory cache `stats.Cache`. On server restarts, the cache started completely empty and returned `0` without ever querying PostgreSQL on a cache miss.

Fixed by updating `svc.Stats()` to query PostgreSQL `store.AccountStats` on a cold cache miss and populate `stats.Cache` via `c.Set()`.

<img width="764" height="454" alt="Screenshot 2026-08-19 at 6 58 57 PM" src="https://github.com/user-attachments/assets/998da18d-8f7d-42aa-809e-fe1e08da14bd" />

### 5. Cache data race
`Cache.Record()` was writing to a shared map without holding the write
lock. `Cache.Get()` had the lock but `Record()` didn't. Under concurrent
requests this would corrupt the counts or crash. Fixed by adding
`c.mu.Lock()` to `Record()`.

## Why Postgres UNIQUE over alternatives?

- **App-level check (`EventExists` before insert):** Vulnerable to TOCTOU race conditions under concurrent requests.
- **Redis lock (`SETNX`):** Fast, but if Redis restarts or evicts keys under memory pressure, duplicates slip through to the DB.
- **Postgres UNIQUE index + `ON CONFLICT DO NOTHING` (Chosen):** 100% atomic ACID deduplication at the storage layer with zero race conditions or extra infrastructure dependencies.

---

## Scaling to 10,000 webhooks/sec

To handle 10,000 req/sec without choking PostgreSQL connection pools:

1. **Edge Ingestion:** Use Redis `SETNX event_id` at the HTTP layer for fast sub-5ms deduplication checks.
2. **Buffer Queue:** Push valid events to a message queue (**Kafka / SQS**) and return `202 Accepted` immediately.
3. **Batch DB Consumers:** Background workers process Kafka events in bulk batches (`COPY` / bulk upsert) to minimize DB lock contention.
4. **Redis Stats Caching:** Maintain running stats in Redis (`HINCRBY`) with write-behind background sync to PostgreSQL.

## Tracked issues and pull requests

I filed a GitHub issue for each bug before fixing it, then opened a PR per fix so the history is easy to follow.

| Issue | PR | What it fixed |
|---|---|---|
| [#1 Duplicate records and stats drifting](https://github.com/manu-r12/webhook-ingest/issues/1) | [#PR](link) | UNIQUE constraint + `ON CONFLICT DO NOTHING` + `IngestTx` transaction |
| [#2 Recordings never marked processed](https://github.com/manu-r12/webhook-ingest/issues/2) | [#PR](link) | `context.WithoutCancel` + error logging |
| [#4 In-flight tasks lost on shutdown](https://github.com/manu-r12/webhook-ingest/issues/4) | [#PR](link) | `sync.WaitGroup` drain on `Close()` |
| [#6 Stats return zero after restart](https://github.com/manu-r12/webhook-ingest/issues/6) | [#PR](link) | Postgres fallback on cache miss |

---

## Additional Quality Enhancements

- **Automated CI/CD Pipeline:** Added GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) to automatically spin up PostgreSQL 16 & Redis 7 service containers and execute `go test -v -race ./...` on every push and PR.
- **End-to-End Simulation Runner:** Built a high-concurrency simulation runner ([`cmd/simulate/main.go`](cmd/simulate/main.go)) that stress-tests 150 concurrent webhooks (50 calls × 3 redeliveries/updates) across 4 live verification phases.

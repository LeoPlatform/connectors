# Plan: Session/Connection Pooling for the Datalake Connector

## Context

The datalake connector's top-level `query()` opens a brand-new connection on **every call**:
`client.connect()` does `new DBSQLClient()` + `sqlClient.connect()` (TLS handshake + OAuth M2M
client-credentials token fetch) + `openSession()` (`lib/connect.js:33-72`), runs one statement on a
session-scoped sub-client (`createSessionClient`, `lib/connect.js:241-278`), then `release()` closes
only the session — never `sqlClient.close()`.

Verified against `@databricks/sql` 1.15.0: the OAuth token cache and the keep-alive HTTP `Agent`
(`maxSockets: Infinity`, `keepAliveMsecs: 10000`) are **both per-`DBSQLClient`**. So a new client per
query re-fetches an OAuth token and opens a new socket every time — and `load.js` drives this under
`async.parallelLimit(tasks, 10)` (`common/datawarehouse/load.js:242,308`), with ~2-3 warehouse queries
per table per micro-batch. The postgres sibling avoids all of this with `pg.Pool`
(`../postgres/lib/connect.js:26-34,223`); the datalake port kept the `connect`/`query`/`release`
interface shape but dropped the pool.

**Goal:** one shared long-lived `DBSQLClient` + a bounded pool of reusable Databricks SQL sessions,
with validate-on-borrow and a bounded idempotent retry so the connector is resilient to a
stopped/cold/severed serverless warehouse — and so the client-side concurrent-query ceiling is
explicit and observable. Add a configurable, floored+capped **10-minute** statement timeout as a
runaway guard.

**Scope:** confined entirely to `connectors/datalake/`. No change to `../postgres`, `../redshift`,
`../common/datawarehouse`, or the shared `offload_to_redshift.js` path. The Redshift pipeline stays
independently deployable. The connector interface (`connect`/`query`/`describeTable`/`describeTables`/
`release`/`disconnect`/`end` + dwconnect's `importFact`/`importDimension`/`insertMissingDimensions`/
`linkDimensions`/`dropTempTables`) is preserved.

## Decisions (locked 2026-05-29)

| # | Decision | Rationale |
|---|---|---|
| Pool | **`generic-pool`** (new dep, approved by Paul 2026-05-29) | Closest to the `pg.Pool` feature set; built-in acquire-timeout, idle eviction, `testOnBorrow`, and `pool.borrowed`/`pending`/`size` stats for the concurrency observability we want |
| Reuse model | **Per-invocation pool** (not module-level) | Mirrors postgres; captures the dominant within-run reuse under `parallelLimit(10)`; avoids global mutable state and Lambda freeze/thaw socket-staleness handling for marginal gain |
| Resilience | **destroy-on-error + bounded idempotent retry on a fresh session** (validate-on-borrow = trivial `!dead` flag, NOT predictive) | SDK does NOT retry `ExecuteStatement`/`OpenSession` or connection severances (see Verified facts); the connector must own this. Session liveness is unknowable without using the session, so detect-on-use rather than predict — no idle-age threshold to keep in sync with the warehouse |
| Timeout | **`STATEMENT_TIMEOUT` session param**, configurable, floored 30s / capped 1800s, default 600s | `executeStatement.queryTimeout` is documented ineffective on SQL Warehouses; this connector targets a serverless warehouse |

## Verified facts (do not re-derive)

- `@databricks/sql` 1.15.0 defaults (`node_modules/@databricks/sql/dist/DBSQLClient.js:111-119`):
  `socketTimeout` **900000ms (15m)**, `retryMaxAttempts` 5, `retriesTimeout` 900000ms, backoff
  1s→60s, `directResultsDefaultMaxRows` 100000.
- SDK retry is narrow (`connection/connections/HttpRetryPolicy.js:69-77` +
  `ThriftHttpConnection.js:49-64`): only HTTP 429 / 5xx(≠501) / <100, **and only for whitelisted
  Thrift methods**. `ExecuteStatement` and `OpenSession` are **not** whitelisted; `GetOperationStatus`
  and close/metadata calls are. Connection severances (node-fetch `FetchError`) propagate **unretried**
  (`ThriftHttpConnection.js:120`).
- `queryTimeout` is "Effective only with Compute clusters. For SQL Warehouses, `STATEMENT_TIMEOUT`
  configuration should be used" (`contracts/IDBSQLSession.d.ts:6-13`). Unit = **seconds**, 0 = disabled.
- The consuming bot already sets `context.callbackWaitsForEmptyEventLoop = false`
  (`general/lib/offload_to_redshift.js:434`), so a pool with keep-alive sockets won't hang the Lambda.
  Nothing calls the connector's `end()`/`disconnect()` today.
- `generic-pool`/`tarn` not currently installed — genuinely new dependency.

## Implementation steps (each independently verifiable)

### Step 1 — `package.json`: add dependency
Add `generic-pool` (`^3.9.0`) to `connectors/datalake/package.json` dependencies; `npm install`
(use `--legacy-peer-deps` to match the existing tree's pre-existing leo-sdk peer state).
Verify: `npm ls generic-pool` resolves; `npm run lint` + `npm test` still green.

### Step 2 — `lib/connect.js`: config surface (factory top, ~line 13)
Normalize new config in `module.exports = function(config)`:
- `poolMax = clamp(config.poolMax, 1, 50, default 10)` — aligned to `load.js parallelLimit(10)`.
  Tuning knob, **not** a safety gate.
- `statementTimeoutSeconds = clamp(config.statementTimeoutSeconds, floor 5, cap 1800, default 600)`.
  `0`/disable is **not** honored (floored to 5) — runaway guard, not optional.
- `acquireTimeoutMillis` (default 30000) so a stuck acquire rejects instead of hanging.
- `socketTimeout` for the shared client = `(statementTimeoutSeconds + 120) * 1000` — pinned **above**
  the statement-timeout cap so a long statement hits the clean `STATEMENT_TIMEOUT` abort, not an
  unretried socket severance (SDK default 900s < 1800s cap — see Risks).

### Step 3 — `lib/connect.js`: shared client, single memoized connect (CORRECTNESS, not optimization)
- Construct `const sqlClient = new DBSQLClient()` **once** at factory scope (move `connect.js:34`).
- Memoize a single `connectPromise`: first `factory.create()` sets
  `connectPromise = connectPromise || sqlClient.connect(connOpts)` and awaits it; all later creates
  await the same promise.
- **Without this single-promise guard, the first `parallelLimit(10)` burst fires 10 `sqlClient.connect()`
  calls → 10 token fetches + 10 sockets, defeating the refactor.**
- The factory stays synchronous and returns the client object (no network at construct time — required
  by the pure-helper unit tests, `test/unit/connect.test.js:67-202`). `connOpts` (M2M vs PAT branch,
  `connect.js:38-50`) + `socketTimeout` computed once.

### Step 4 — `lib/connect.js`: pool construction (per-invocation, inside the factory)
- `factory.create()`: `await connectPromise`, then
  `sqlClient.openSession({ initialCatalog, initialSchema, initialParameters })`. The
  `initialParameters` block (`connect.js:62-66`) keeps `ansi_mode`, `infer_timestamp_ntz_type`,
  `timezone` and **adds `STATEMENT_TIMEOUT: String(statementTimeoutSeconds)`**. Wrap the session
  (`createSessionClient` shape) and stamp `wrapper.dead = false`.
- `factory.destroy(wrapper)`: best-effort `wrapper._session.close()` (swallow errors, as today at
  `connect.js:272`).
- `factory.validate(wrapper)`: Step 5.
- Pool opts: `{ max: poolMax, min: 0, testOnBorrow: true, acquireTimeoutMillis, autostart: false }`.
  No idle eviction (`evictionRunIntervalMillis` off) — a per-invocation pool is torn down at `end()`,
  so idle reaping is irrelevant and would only be a (warehouse-coupled) guess.

### Step 5 — `lib/connect.js`: validate-on-borrow (trivial `!dead` flag, NOT predictive)
- `validate(wrapper)` → `wrapper.dead !== true`. No network round-trip, no time-based threshold.
- **Why not idle-age:** session liveness is unknowable without using the session (a server-side-closed
  session or dropped socket isn't visible client-side until a send fails). An idle-age threshold is a
  guess that would have to be kept in sync with the warehouse auto-stop / server session-idle timeout —
  the tunable-infra anti-pattern. Too low evicts healthy sessions; too high hands out dead ones.
- **The real liveness mechanism is detect-on-use:** `query()` destroys a session on any
  connection-class error (Step 7), and the bounded idempotent retry (Step 8) re-acquires a fresh one.
  The retry *is* the validation — against ground truth, zero config. Since destroy-on-error already
  removes dead sessions, the `!dead` check is belt-and-suspenders.
- **Alternative not chosen:** a `SELECT 1` ping per acquire would guarantee never *attempting* on a
  dead session, but adds a round-trip on every borrow — a real tax under the tight `parallelLimit(10)`
  loop. Rejected; the retry recovers cheaply on the rare stale borrow. (This is the lever if a hard
  "never attempt on a dead session" guarantee is ever required.)

### Step 6 — `lib/connect.js`: `query()` routes through acquire/release (`connect.js:81-97`)
- Keep the callback-normalizing arg shuffle (`82-89`). Then `pool.acquire()` → run
  `wrapper.query(sql, params, cb')`; `cb'` classifies the error (Step 7), releases-or-destroys, then
  calls the original `cb`. On `acquire()` rejection
  (acquire-timeout / connect failure) call `cb(err)`.
- `createSessionClient` keeps its `executeStatement`→`fetchAll`→`operation.close` body
  (`connect.js:255-261`); its `release()` no longer closes the session — the pool owns lifecycle.
- All query paths already funnel here: `describeTables` (`connect.js:128`), `ensureStagingLocation`
  (`connect.js:310`), and dwconnect's `alterAddColumn` (`95`), `createTable` (`109`), MIN (`224`),
  flushDeletes (`302`), MERGE (`320`). `streamToTableFromS3` (`198`) is S3-only — no session.

### Step 7 — `lib/connect.js`: error classification (`isConnectionError`)
- Connection-class (node-fetch `FetchError`, `ECONNRESET`/"socket hang up", "session closed", thrift
  transport errors): `wrapper.dead = true`, `pool.destroy(wrapper)`, then `cb(err)` — never return a
  poisoned session.
- Query-class (SQL syntax/permission/data): `pool.release(wrapper)` healthy, `cb(err)`.
- Implement a small isolated `isConnectionError(err)` predicate (name/code/message checks);
  conservative default = treat unknown transport errors as connection-class (destroy+retry is the safe
  failure mode for idempotent MIN/MERGE). Unit-test the predicate directly.

### Step 8 — `lib/dwconnect.js`: bounded idempotent retry around MIN/MERGE
- Add `withRetry(fn, { attempts: 3, isRetryable })` and wrap the MIN submit (`dwconnect.js:224-229`),
  the MERGE submit (`doMerge`, `320`), and the flushDeletes UPDATE (`302`, idempotent: repeated
  `SET _deleted=true ... WHERE id IN (...)`).
- Retry only on connection-class errors and the rare `ExecuteStatement` 429/5xx at submit (SDK won't).
  Each retry re-calls `client.query`, acquiring a **fresh** session (the dead one was destroyed in
  Step 7). Small backoff.
- Safe because staging is S3-first (CSV already uploaded before MIN/MERGE) and Delta MERGE is
  atomic/idempotent — re-running yields the same end state. Keep retry at **this** layer, never inside
  `query()` (which runs arbitrary, possibly non-idempotent statements) — preserves the porting-discipline
  no-flattening rule (`CLAUDE.md`).

### Step 9 — `lib/connect.js`: real `disconnect`/`end` + observability (`connect.js:74-78`)
- `end`/`disconnect`: `await pool.drain(); await pool.clear(); await sqlClient.close()`
  (idempotent/guarded). These are no longer safe no-ops once the client is long-lived.
  `release()` stays a top-level no-op (callers here don't hold a checked-out client).
- Observability: sample `pool.borrowed`/`pending`/`size`/`available` at each acquire; log peak
  `borrowed` and peak `pending` via `leo-logger` at drain. Makes the client-side concurrency ceiling
  visible and shows whether `poolMax` binds under `parallelLimit(10)`.
- Bot guidance (for the future datalake offload bot, not the running redshift bot): call
  `client.end()` in a `finally` after the load pipeline completes. If omitted, the per-invocation pool
  relies on Lambda freeze with sockets open (`callbackWaitsForEmptyEventLoop = false` already set) —
  acceptable but suboptimal; prefer the explicit `end()`.

## Follow-up: module-level DBSQLClient

The per-invocation pool creates a new `DBSQLClient` on each `connectFactory()` call, which means a
fresh OAuth token fetch + TLS handshake per Lambda invocation. The plan chose this to mirror
`pg.Pool` lifecycle and avoid freeze/thaw staleness reasoning.

However, the SDK's `TelemetryClient` warning (`a second DBSQLClient registered with a different
auth provider`) is a real signal: in a warm Lambda container, multiple invocations each create a
new client for the same host. The staleness concern applies to **sessions** (pool's validate-on-
borrow handles that) but not to the `DBSQLClient` itself — the keep-alive Agent reconnects on a
stale socket, and the OAuth token cache is designed to survive across calls.

An incremental improvement: move `DBSQLClient` + `ensureConnected` to module scope; keep the
session pool per-invocation (still drains at `end()`, sessions still fresh per run). Deferred —
the `telemetryEnabled: false` fix suppresses the warning for now. Revisit once production
invocation cadence and OAuth overhead are characterized.

## Risks & edge cases

1. **`socketTimeout` (900s) < `STATEMENT_TIMEOUT` cap (1800s)** — unaddressed, a long statement dies as
   an unretried socket severance instead of a clean timeout. Mitigated in Step 2 (pin socketTimeout
   above the cap). Verify on a warehouse.
2. **`STATEMENT_TIMEOUT` may not fire as expected on a serverless warehouse** — `queryTimeout` is
   ineffective there; `STATEMENT_TIMEOUT` via `initialParameters` is the correct lever but must be
   **empirically verified** (deliberately slow statement aborts at the bound).
3. **Memoized-connect race** — if not a single shared promise, the initial burst re-fetches tokens
   (Step 3). Unit test asserts a single `sqlClient.connect`.
4. **Error misclassification** — both directions harmful (churn vs poison). Mitigated by the isolated,
   unit-tested `isConnectionError` predicate (Step 7).
5. **Draining mid-flight** — `end()` only after the final load callback; `acquireTimeoutMillis` prevents
   indefinite hangs.
6. **No shared mutable state on the pool/client** — do not park per-call values on the shared object
   (the connector already moved staging identifiers to caller-owned; `connect.js:180-191`,
   `dwconnect.js:189-201`). Pool/sessions are purpose-built concurrency primitives, fine to share.
7. **Lambda freeze/thaw** — per-invocation pool sidesteps cross-invocation socket staleness;
   destroy-on-error + bounded retry cover within-run staleness (detect-on-use, no idle-age threshold).
   Do **not** design around warehouse auto-stop or a session-idle timeout (tunable knobs, changeable).

## Reuse / precedent
- `../postgres/lib/connect.js` — `new Pool({…max})` (`26-34`), `pool.connect()` (`51`), `pool.query()`
  (`223`), `disconnect`/`end` = `pool.end` (`105-106`), `release` = `pool.release` (`284-285`). Mirror
  this shape with generic-pool over Databricks sessions.
- `createSessionClient` (`lib/connect.js:241-278`) — already the per-session wrapper; becomes the
  pooled resource.

## Verification
1. `npm run lint` (eslint) — clean.
2. `npm test` (mocha unit, currently 111 passing) — clean. New/updated unit tests:
   - no network on factory construct; single `sqlClient.connect` under an N-way burst; acquire/release
     accounting (borrowed returns to 0); `isConnectionError` across error shapes (SQL→release,
     conn→destroy); `initialParameters.STATEMENT_TIMEOUT` = clamped value (0→30, 99999→1800, default
     600); `poolMax` clamp; `disconnect`/`end` invoke drain+clear+close idempotently; socketTimeout >
     statementTimeout*1000; dwconnect retry succeeds-after-one-conn-failure, does NOT retry SQL errors,
     is bounded, and re-acquires a fresh session per attempt.
3. Integration (`test/integration/`, skip-when-unconfigured as in `timezone.test.js:23-31`, nonprod
   allowlist `helpers/databricks.js`):
   - **`STATEMENT_TIMEOUT` actually aborts** a deliberately slow statement at a low configured bound on
     the serverless warehouse (the critical unknown).
   - Pooling reuse: many queries through one factory → single token fetch / socket (assert 2nd+ query
     wall-clock materially lower; log peak borrowed).
   - Detect-on-use recovery: induce a connection-class error on a borrowed session → assert it is
     destroyed (not returned), the bounded retry acquires a fresh session, the operation succeeds, and
     no error surfaces to the caller.
   - Concurrency: loads at `parallelLimit(10)` complete; borrowed never exceeds `poolMax`; no cross-talk
     between concurrent staging paths.
   - Idempotency: re-running the same MERGE leaves the table unchanged (extend `idempotency.test.js`).

## Critical files
- `lib/connect.js` — shared client, memoized connect, pool, validate, `query()` routing, error
  classification, `STATEMENT_TIMEOUT`, `disconnect`/`end`, observability.
- `lib/dwconnect.js` — bounded retry around MIN/MERGE/flushDeletes.
- `package.json` — `generic-pool`.
- `test/unit/connect.test.js`, `test/unit/dwconnect.test.js`, `test/integration/` — tests above.

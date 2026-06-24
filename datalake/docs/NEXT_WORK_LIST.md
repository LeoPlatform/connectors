# Datalake connector — work list (next session)

Consolidates three sources reviewed 2026-06-22:

1. Refreshed datalake-vs-postgres divergence review (post session-reuse, commit `b62d3b8`).
2. [BUILD_PLAN.md](BUILD_PLAN.md) items not yet implemented.
3. [BUILD_PLAN.md](BUILD_PLAN.md) items intentionally deferred.

Items are tagged **[Bug]** (correctness regression vs postgres), **[Gap]** (BUILD_PLAN step not yet built), **[Defer]** (intentionally out of scope), or **[Doc]** (documentation drift).

---

## 1. Refreshed divergence review

### 1a. Forced and legitimate divergences (no action — they're correct)

| Area | Postgres | Datalake | Why divergent |
|---|---|---|---|
| Staging artifact | `staging_<table>` real table | S3 file, inlined via `read_files()` in MERGE | Each pooled session can serve any query; a session-scoped temp view from one acquire isn't guaranteed to live across the MIN+MERGE pair |
| UPSERT mechanism | `UPDATE prev FROM staging` + `INSERT WHERE NOT EXISTS` | `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED INSERT` | Delta supports atomic MERGE; Redshift doesn't |
| `naturalKeyFilter` placement | WHERE clauses on `prev` joins / DELETE | Not used — Redshift block-skipping via SORTKEY has no equivalent; Delta file-skipping is driven by `CLUSTER BY AUTO` + Photon column stats | Databricks Photon handles this automatically |
| `escapeId` casing | preserves case (`"Name"`) | lowercases (`` `name` ``) | Intentional lowercase-everywhere convention ([CLAUDE.md](../CLAUDE.md)); BUILD_PLAN Step 3 open question #7 |
| Staging identifier | `qualifiedStagingTable = "${stageSchema}.${stageTablePrefix}_${table}"` | `stagingS3Path(s3Bucket, s3Prefix, table, auditdate)` | Different shape (URI vs schema-qualified name), same ownership pattern — caller owns and passes down |
| Transactions | `BEGIN/COMMIT/ROLLBACK` around post-stage SQL | None | Pool sessions are independent acquires per query; multi-statement TX not viable in this model |
| Enrichment | At INSERT-SQL time (`SELECT staging.*, false AS _deleted, ${auditdate} AS _auditdate`) | Per-row in `enrichedStream` (datalake/lib/dwconnect.js:179-187) | Hashed surrogate keys force a per-row JS pass; folding audit values in there is natural |
| Cleanup | Module-level `tempTables[]` + `dropTempTables()` API | Inline `cleanupStagedFile` after MERGE | S3 files must be deleted promptly; postgres temp tables can persist until orchestrator tears down |

### 1b. New since session-reuse (commit `b62d3b8`) — analogous to postgres `pg.Pool`

| Area | Postgres | Datalake (post session-reuse) | Status |
|---|---|---|---|
| Connection model | `pg.Pool({ max })` | Shared `DBSQLClient` + `generic-pool` of sessions, validate-on-borrow, destroy-on-error | ✓ Analogous shape |
| Memoized connect | implicit in pg.Pool | Single `connectPromise` so the first `parallelLimit(10)` burst doesn't fire 10 OAuth fetches | ✓ Net-new but justified |
| `withRetry` bounded idempotent retry | not needed (pg has its own) | Wraps MIN, MERGE, flushDeletes (datalake/lib/dwconnect.js:213, 225, 229, 232) | ✓ Net-new, justified — SDK doesn't retry ExecuteStatement/OpenSession or connection severances |
| `STATEMENT_TIMEOUT` runaway guard | implicit (Redshift WLM/QMR) | `STATEMENT_TIMEOUT` session param, floored 5s, capped 1800s, default 600s | ✓ Net-new, justified — serverless warehouse |
| `isConnectionError` classifier | implicit (pg error codes) | Explicit predicate (datalake/lib/connect.js:27) drives destroy-on-error vs release-healthy | ✓ Net-new, justified |
| `disconnect`/`end` | `pool.end()` | `pool.drain() → pool.clear() → sqlClient.close()`, raced against `drainTimeoutMs` | ✓ Analogous |

### 1c. Different but accepted (per [porting-decisions.md](porting-decisions.md))

Closed list of items deliberately left matching postgres. Don't re-flag without showing a concrete failure mode in the datalake context:

- `flushDeletes` IN-list via string interpolation
- `alterColumnType` helper exists but is never called from `changeTableStructure`
- `escapeValue` lowercases (dead code in datalake; OrderStream/Dsco convention if ever wired)
- Schema cache invalidated only on `createTable`, not on `ADD COLUMN`
- `npm` scripts use shell globs
- `dwClient = client` module-scope alias
- `naiveIsoNow()` second-resolution (load-bearing — see CLAUDE.md timestamp section)
- `escapeId('')` returns `` `` ``

### 1d. Residual issues I flagged earlier that the session-reuse commit did NOT fix

These are real and still present after `b62d3b8`. Worth addressing:

- ~~**[Bug] `flushDeletes` post-retry error is swallowed.**~~ ✓ Fixed — `mergeCallback` hoisted before `withRetry(flushDeletes...)`; callback now takes `(flushErr)` and returns early. Unit-tested: `propagates a flushDeletes error and skips MERGE`.

- ~~**[Bug] `count` returned to the orchestrator is wrong.**~~ ✓ Fixed — `stagingCount` captured from a `SELECT CAST(COUNT(*) AS INT)` before the MERGE; `doMerge` simplified to `client.query(mergeSql, [], callback)`. Unit-tested. Integration-tested: `loads 100 records via importFact` now asserts `tableInfo.count === 100` and passes against live Databricks.

- ~~**[Removed] `clusterKey` / `naturalKeyFilter` / `pruneCol` apparatus.**~~ ✓ Removed — the `clusterKey` dw_fields field, `MIN(clusterKey)` pruning query, `literalForType` helper, and all associated parameters dropped. Every table now declares `CLUSTER BY AUTO`; Databricks Photon handles file-skipping via transaction-log column statistics driven by clustering.

- ~~**[Doc] Outdated `streamToTableFromS3` comment.**~~ ✓ Done — was actually in [dwconnect.js:207-209](../lib/dwconnect.js#L207-L209), not connect.js. Updated the rationale to "Sessions are pooled, so the MIN and MERGE queries may run on different acquires — a session-scoped temp view from one acquire is not guaranteed visible to the next. Inlining avoids that."

### 1e. Remaining dimension gaps

- **[Fixed] `insertMissingDimensions` was throwing instead of no-op.** Under `hashedSurrogateKeys=true`, `postgres/lib/dwconnect.js:680` immediately calls `callback(null)` — stub placeholder rows aren't needed because any FK reference computes the same hash and merges correctly when the dim row arrives. The datalake stub was incorrectly throwing. Fixed: now `callback(null)` with an explanatory comment. See [porting-decisions.md](porting-decisions.md).

- **[Doc addressed] `_startdate/_enddate/_current` columns are never written.** `createTable` adds these for dim tables to mirror the postgres schema, but SCD is bypassed in all production configs (`bypassSlowlyChangingDimensions=true`, no `scds` fields in any `dw_fields`). These columns will be null until a dim upsert path is built. Clarified in the `sql.js` JSDoc.

- ~~**[Gap] `importDimension` not yet implemented.**~~ ✓ Implemented — bypassSCD dim upsert using shared `stageToS3` helper + `sql.mergeDim`. Sentinel values for new rows match postgres bypass path (`_current=true`, `_startdate='1900-01-01 00:00:00'`, `_enddate='9999-01-01 00:00:00'`). `__leo_delete__` markers filtered from staging path; soft-close deferred (no dim queue generates deletes under `bypassSlowlyChangingDimensions=true`). 13 unit tests added. `importFact` refactored to share the same `stageToS3` helper with no behavior change. `linkDimensions` still unimplemented — see item below.

- ~~**[Gap] `linkDimensions` not yet implemented.**~~ ✓ Implemented — FK surrogate-key values pre-computed in `importFact`/`importDimension` enrichFns (`buildFkEnrichers`) and written into staging CSV before MERGE. `linkDimensions` is now a no-op. `sql.js` `createTable` and `changeTableStructure` both emit/check FK columns. Date/time surrogate keys use JS wall-clock math (`dateSk`/`timeSk`) rather than SQL (no Databricks `FARMFINGERPRINT64`). See `docs/porting-decisions.md`. 20 unit tests added. All 188 unit tests pass.

---

## 2. BUILD_PLAN.md items not yet implemented

### ~~[Gap] Step 8 — `test/unit/load.smoke.test.js` (offline smoke through real `load.js`)~~ ✓ Done
6 unit tests added. Pipes 100 synthetic events through the real `load.js` (including `combine.js` sort-and-dedup) into the real `lib/dwconnect.js importFact`, with `connect.js` and S3 stubbed at the `connect.js` boundary. Asserts the expected call sequence: `streamToTableFromS3`, MIN prune query, MERGE INTO, `insertMissingDimensions`, `dropTempTables`. 194 unit tests pass, lint clean.

### [Gap] Step 9 — CI workflow scaffolding (per-branch cloned catalog)
BUILD_PLAN Step 9 calls for two GitHub Actions workflows paralleling `data-lake-datapipelines`: `create-catalog.yml` on branch `create` and `destroy-catalog.yml` on branch `delete`, both using `chub-engineering/commercehub-actions/data-lake/shallow_clone` / `destroy`. **Not present** in `.github/workflows/`. Implication: today's integration tests only run locally against `de_cup_dev_us` / `public_stage_local`; CI doesn't validate against a per-branch isolated catalog.

### [Gap] Step 12 — Equivalence script (DoD check)
[test/equivalence/run.js](../test/equivalence/run.js) exists as a stub that errors out. Blocked on:
- Open question #3 — captured PII-scrubbed prod fixture + nonprod environments live
- Open question #6 — `READ_FILES` grant on the relevant External Location
- Locking the table coverage set (see Step 12 criteria)
- Lakebridge invocation + hand-rolled MD5 row-level diff implementation

This is the documented DoD gate. Not flippable until the captured fixture exists.

### [Gap] Open question #3 follow-ups (CI plumbing)
Per BUILD_PLAN open question #3 (otherwise resolved):
- SQL warehouse HTTP path
- Service-principal credentials path in Secrets Manager (pattern: `data-emporium/dev/ci/<repo>/variables/*`)
- GitHub Actions → AWS OIDC trust grant for this repo

All needed before the Step 9 CI workflows can authenticate.

### [Gap] Open question #6 follow-up — `READ_FILES` grant
External Location `datalake-dev-external-location` exists (`infra-iac-databricks/data-platform/main.tf:263`) and covers the staging bucket. The `[dev-cup]` SP currently works for local dev. For CI, either: (a) reuse the `dbt` SP (already has `READ_FILES`), or (b) add a new SP + grant in `infra-iac-databricks/`. Decision deferred.

### [Gap] BUILD_PLAN risk #4 — `offload_to_datalake.js` bot
Without this bot in `general/`, nothing runs in production. The connector library is complete for fact tables; dim tables additionally need `importDimension` and `linkDimensions` (see §3 Dimension code paths). Shortest deployment path: write the bot, run it against `supplier-catalog-dim` first (fact-only queue), then add dim queue support once `importDimension` is implemented.

### [Gap] BUILD_PLAN risk #5 — Operational monitors / alerts
CloudWatch alarms, Datadog monitors, dashboards, PagerDuty services watching Redshift pipeline health need Databricks counterparts before Redshift retires. Inventory not started.

### [Gap] BUILD_PLAN risk #6 — Redshift-specific AI Arcanum skills audit
Skills that embed Redshift SQL or connection strings will produce wrong output once Databricks is live. Audit not started.

### [Gap] BUILD_PLAN risk #3 — `bus-models/dw-schema` lowercase-assertion deploy gate
Mentioned in Step 1 prerequisites: extend `bus-models/dw-schema` with the lowercase-keys assertion. Without it, a producer adding a mixed-case key to `dw_fields` would silently diverge (Databricks lowercases on write; Redshift tolerates either casing; consumer queries that case-match Redshift diverge on Databricks). Implementation status in `bus-models` unknown — needs verification.

---

## 3. Intentionally deferred in BUILD_PLAN.md (don't pull forward)

Per BUILD_PLAN's "this plan covers Deliverable #1 only":

### [Defer] Deliverable #2 — VARIANT event tables
Out of scope for this connector. Different storage shape (semi-structured), different staging path. Reopen when explicitly scoped.

### [Defer] Deliverable #3 — Retabulation library
Out of scope. Reopen when explicitly scoped.

### [Defer] `checksum.js` — Checksum bot path
[lib/checksum.js](../lib/checksum.js) is a `throw new Error('checksum not implemented')` stub. Per BUILD_PLAN open question #4: consumed by a separate checksum bot, not the loader hot path. Defer until a checksum bot migration is requested.

### [Defer] `streamToTable` (non-S3 direct-write path)
[datalake/lib/connect.js:349-351](../lib/connect.js#L349-L351) throws. Postgres has a non-Redshift branch that uses direct COPY; datalake doesn't need it — all paths go through S3 staging. Keep as `throw` for interface parity.

### [Defer] `alterColumnType` wiring
Per [porting-decisions.md](porting-decisions.md): postgres also doesn't wire it. Defer indefinitely; only file a ticket if a dw_fields type evolution actually needs it.

### [Defer] Module-level `DBSQLClient`
SESSION_REUSE_PLAN.md follow-up section. Current implementation creates a new `DBSQLClient` per `connectFactory()` call (per-invocation pool); could be hoisted to module scope to amortize OAuth fetch + TLS handshake across Lambda invocations in a warm container. `telemetryEnabled: false` suppresses the warning for now. Revisit once production invocation cadence and OAuth overhead are characterized — not before.

### [Defer] Strict mode (`ansi_mode = true`)
BUILD_PLAN Step 3 rationale: kept lenient for Redshift `ACCEPTINVCHARS`/`ACCEPTANYDATE` parity during coexistence. Modernization choice to revisit after Redshift retires.

### [Done] `rescuedDataColumn`
Enabled. `_rescued_data STRING` is added to every table by `createTable` and by the startup schema reconciliation loop for existing tables. `buildStagingSelect` passes `rescuedDataColumn => '_rescued_data'` to `read_files()`. Both `mergeFact` and `mergeDim` include it in UPDATE SET (overwrites on each merge — null when record was clean) and INSERT. The column is on Databricks tables only; consumers doing cross-pipeline `SELECT *` comparisons will see it on the Databricks side and not on Redshift. If that divergence is a problem, drop the column — the connector degrades gracefully back to silent-null behavior.

### [Defer] Auto Loader
BUILD_PLAN Step 7.2 rationale: RStreams already provides exactly-once + checkpointing; Auto Loader would duplicate state. Revisit only if `read_files` proves unreliable at scale.

---

## Quick-action triage for next session

All §1d correctness bugs are fixed. §1e items are addressed. Next items by deployment readiness:

1. Write `offload_to_datalake.js` bot in `general/` — the library is ready; nothing runs without it.
2. ~~Step 8 smoke test (`test/unit/load.smoke.test.js`) — small regression guard before wiring the bot.~~ ✓ Done.

**Blocked on infra/fixture inputs (don't pull forward):**
- Step 9 CI catalog cloning workflows
- Step 12 equivalence script (formal DoD gate)

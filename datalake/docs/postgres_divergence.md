# Datalake vs. postgres — structural divergence reference

The datalake connector is a fork of the postgres connector (`../../postgres/`) adapted for Databricks. This is the at-a-glance map of *how and why* the two differ in ways that are **correct by design** — not bugs, not pending work.

How this relates to the other docs:
- **[porting_decisions.md](porting_decisions.md)** — the closed "don't re-flag in review" list: verbatim ports plus small deliberate divergences, each with the rationale a reviewer needs.
- **[../CLAUDE.md](../CLAUDE.md)** — in-depth treatment of the *load-bearing* divergences (TIMESTAMP_NTZ semantics, Delta-native MERGE, FarmFingerprint-in-Node, S3 staging). Read it before touching any of those paths.
- **This file** — the consolidated single-page overview that ties them together; useful when onboarding or reasoning about why a datalake helper doesn't match its postgres sibling.

Remaining migration work (offload bot, CI/infra, equivalence validation, Redshift-retirement gates, deferred connector enhancements) is tracked outside this repo, as part of the EDW migration. The connector library build is complete — [build_plan.md](build_plan.md).

---

## Forced and legitimate divergences (correct — not action items)

| Area | Postgres | Datalake | Why divergent |
|---|---|---|---|
| Staging artifact | `staging_<table>` real table | S3 file, inlined via `read_files()` in MERGE | Each pooled session can serve any query; a session-scoped temp view from one acquire isn't guaranteed to live across the MIN+MERGE pair |
| UPSERT mechanism | `UPDATE prev FROM staging` + `INSERT WHERE NOT EXISTS` | `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED INSERT` | Delta supports atomic MERGE; Redshift doesn't |
| Scan pruning | `MIN(staging_col) >= target_col` predicate for SORTKEY block-skipping | None — `CLUSTER BY AUTO` + Photon column stats handle file-skipping | Redshift SORTKEY pruning has no Delta equivalent (see [porting_decisions.md](porting_decisions.md)) |
| `escapeId` casing | preserves case (`"Name"`) | lowercases (`` `name` ``) | Intentional lowercase-everywhere convention ([../CLAUDE.md](../CLAUDE.md)); build_plan Step 3 open question #7 |
| String-literal escaping | `\'` quoting (postgres backslash-escape mode) | `\'` quoting, but backslashes doubled first | Datalake escape helpers are public API for the read-side consumers; correctness for all inputs required (see [porting_decisions.md](porting_decisions.md)) |
| Staging identifier | `qualifiedStagingTable = "${stageSchema}.${stageTablePrefix}_${table}"` | `stagingS3Path(s3Bucket, s3Prefix, table, auditdate)` | Different shape (URI vs schema-qualified name), same ownership pattern — caller owns and passes down |
| Transactions | `BEGIN/COMMIT/ROLLBACK` around post-stage SQL | None | Pool sessions are independent acquires per query; multi-statement TX not viable in this model |
| Enrichment | At INSERT-SQL time (`SELECT staging.*, false AS _deleted, ${auditdate} AS _auditdate`) | Per-row in `enrichedStream` (`lib/dwconnect.js`) | Hashed surrogate keys force a per-row JS pass; folding audit values in there is natural |
| Cleanup | Module-level `tempTables[]` + `dropTempTables()` API | Inline `cleanupStagedFile` after MERGE | S3 files must be deleted promptly; postgres temp tables can persist until orchestrator tears down |

## Connection-model additions (session-reuse, commit `b62d3b8`) — analogous to postgres `pg.Pool`

| Area | Postgres | Datalake | Status |
|---|---|---|---|
| Connection model | `pg.Pool({ max })` | Shared `DBSQLClient` + `generic-pool` of sessions, validate-on-borrow, destroy-on-error | Analogous shape |
| Memoized connect | implicit in pg.Pool | Single `connectPromise` so the first `parallelLimit(10)` burst doesn't fire 10 OAuth fetches | Net-new but justified |
| `withRetry` bounded idempotent retry | not needed (pg has its own) | Wraps MERGE, flushDeletes, count | Net-new — SDK doesn't retry ExecuteStatement/OpenSession or connection severances |
| `STATEMENT_TIMEOUT` runaway guard | implicit (Redshift WLM/QMR) | session param, floored 5s, capped 1800s, default 600s | Net-new — serverless warehouse |
| `isConnectionError` classifier | implicit (pg error codes) | Explicit predicate (`lib/connect.js`) drives destroy-on-error vs release-healthy | Net-new |
| `disconnect`/`end` | `pool.end()` | `pool.drain() → pool.clear() → sqlClient.close()`, raced against `drainTimeoutMs` | Analogous |

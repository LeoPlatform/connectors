# Ported-as-is decisions

This is the deliberately-not-fixed list. Items here were flagged in code review but kept matching the postgres sibling (`../../postgres/lib/dwconnect.js`, `../../postgres/lib/connect.js`) on purpose. Re-flagging them as "improvements" wastes review cycles and risks breaking parity with a connector that has been running in production for years.

If you think one of these *should* be changed, the bar is: a concrete failure mode in the datalake context that postgres doesn't have, not stylistic preference. Write up the failure mode in a PR description before changing the code.

## Read this first

The datalake connector is a fork of the postgres connector adapted for Databricks. The contract is: **same `dw_fields`, same loader, same staging-then-merge shape**. When postgres and datalake diverge in a load-bearing way (TIMESTAMP_NTZ semantics, Delta-native MERGE, FarmFingerprint in Node), the divergence is documented in [../CLAUDE.md](../CLAUDE.md). Everything else is intentionally ported verbatim.

## Items intentionally kept matching postgres

### `flushDeletes` builds `WHERE col IN (…)` via string interpolation
- **Location:** `lib/dwconnect.js` — `flushDeletes`
- **Looks like:** `${col} IN (${ids.map(...).join(',')})` with quoting handled inline
- **Why kept:** Same pattern as `postgres/lib/dwconnect.js`. Values originate from internal RStreams `__leo_delete_id__` markers, not user input — the trust boundary is identical to postgres. Switching to parameterized binds would require a Databricks SQL driver feature postgres doesn't use either.

### `alterColumnType` helper exists but `changeTableStructure` never calls it
- **Location:** `lib/sql.js` exports `alterColumnType`; `lib/dwconnect.js` `changeTableStructure` only emits ADD COLUMN
- **Why kept:** Postgres has the same gap — Redshift can't widen in place, so the postgres connector has never had a wired type-widening path. Databricks *could* support this, but wiring it on the datalake side without a postgres counterpart would create a behavioral divergence between the two connectors during coexistence. If a dw_fields type ever changes (e.g. `int` → `bigint`), the Redshift column also stays as-is today; the migration plan handles type evolution out-of-band.
- **The one case worth filing later:** If Databricks-side dw_fields ever introduces a type that postgres can't represent and we need to widen, file a ticket then. Don't pre-wire it.

### `escapeValue` lowercases string values
- **Location:** `lib/connect.js` — `escapeValue` returns `"'" + value.toLowerCase() + "'"`
- **Why kept:** Two reasons.
  1. **Incomplete — `findAuditDate` and `exportChanges` not yet ported.** These are the read-side interface (leoDW / Query Explorer reads audit dates and exports changed rows). They will need to be implemented in the datalake connector when those consumers migrate to Databricks. `escapeValue` is therefore dead today but will be needed when that read path is built.
  2. **The lowercasing is an OrderStream/Dsco convention** (all string data is lowercased before storage), not a bug. If `escapeValue` is ever wired up, that convention has to be preserved for parity with what Redshift consumers expect.

### Schema cache invalidated only on `createTable`, not on `ADD COLUMN`
- **Location:** `lib/dwconnect.js` — `client.clearSchemaCache()` is called only after CREATE
- **Why kept:** Same scope as postgres. The cache is per-process and short-lived; the next loader invocation (a few seconds later) re-reads `information_schema` regardless. Adding invalidation on ADD COLUMN would not be wrong, but it would be a divergence from postgres without a load-bearing reason.

### `npm` scripts use shell globs (`test/unit/**/*.test.js`)
- **Location:** `package.json`
- **Why kept:** Same as postgres. macOS and Linux are the only target platforms for the loader bots. Windows portability is not a project requirement.

### `dwClient = client` module-scope alias
- **Location:** `lib/dwconnect.js` — `let dwClient = client;`
- **Why kept:** Stylistic; ported verbatim. The alias makes the auditdate plumbing read like postgres (`dwClient.auditdate`). Renaming for clarity here would create a noisy diff against the sibling without any behavior change.

### `naiveIsoNow()` emits second-resolution timestamps (no millis, no `Z`)
- **Location:** `lib/audit_timestamp.js`, used by `lib/connect.js` `setAuditdate`
- **Why kept: LOAD-BEARING.** This isn't merely a port — it's required for `_auditdate` parity with Redshift session-UTC `sysdate` values, which is what Redshift consumers compare against during the long coexistence period. The Z-strip is also required for TIMESTAMP_NTZ ingestion via `read_files` (offsets cause PERMISSIVE-mode nulling). See [../CLAUDE.md](../CLAUDE.md) "Timestamp handling — preserving legacy no-TZ semantics".
- **Do not "fix" by adding millis or restoring `Z`.** Both would break parity with Redshift `_auditdate` values, and the `Z` would null out under `infer_timestamp_ntz_type = true`.

### `escapeId('')` returns `` `` `` (empty backticks)
- **Location:** `lib/connect.js` — `escapeId`
- **Why kept:** Defensive ugliness; no caller passes an empty string. Same as postgres. Not worth a guard clause.

### `insertMissingDimensions` is a no-op (not a deferred feature)

- **Location:** `lib/dwconnect.js` — `insertMissingDimensions`
- **Why kept:** Same behavior as `postgres/lib/dwconnect.js:680`, which immediately calls `callback(null)` under `hashedSurrogateKeys=true`. Hashed surrogate keys make stub placeholder rows unnecessary: any FK reference that arrives before its dimension row will compute the same hash, and the dim row will merge correctly when it eventually appears. The datalake connector always uses hashed surrogate keys, so this no-op applies unconditionally. `load.js:246` calls this function for every batch and propagates its error — throwing here breaks all batches; the no-op is correct and intentional.

### `linkDimensions` is a no-op (work moved to enrichFn, not deferred)

- **Location:** `lib/dwconnect.js` — `linkDimensions`
- **Looks like:** Immediate `done(null)` — same shape as `insertMissingDimensions`.
- **Why kept this way:** Postgres `linkDimensions` runs a post-MERGE `UPDATE` using `FARMFINGERPRINT64()` in SQL. Databricks SQL has no `FARMFINGERPRINT64` equivalent. To preserve hash-output parity with the dimension row SK (both computed via `fingerprint64` in `lib/surrogate_key.js`), FK surrogate-key values are instead computed in Node.js inside the `importFact` / `importDimension` enrichFns (`buildFkEnrichers`) and written into the staging CSV before the MERGE. The MERGE then populates the FK columns directly, making a post-MERGE SQL update unnecessary.
- **This is not the same as `insertMissingDimensions`.** `insertMissingDimensions` is a no-op because postgres also skips it under `hashedSurrogateKeys=true`. `linkDimensions` does real work in postgres regardless — the datalake no-op is a platform-forced divergence, not a shared pattern.
- **Do not add a SQL UPDATE path here.** The staging CSV already carries the computed FK values; a redundant UPDATE would overwrite them with the same values but introduce a second round-trip and break the "enrichFn owns FK columns" invariant. If this needs revisiting (e.g. for composite-NK dimensions), extend `buildFkEnrichers` in the enrichFn, not `linkDimensions`.
- **Reference:** `docs/BUILD_PLAN.md` §Step 6 extension.

## Items not on this list

If a reviewer flags something not listed here, it isn't covered by this doc — judge it on its own merits. The list is a closed set, not a presumption of intentionality for everything else.

## Cross-references

- Porting discipline (avoid silently flattening conditionals): [../CLAUDE.md](../CLAUDE.md) under "Exemplar References → Porting discipline"
- Timestamp/TZ rules: [../CLAUDE.md](../CLAUDE.md) under "Timestamp handling — preserving legacy no-TZ semantics"
- Postgres sibling source: [../../postgres/lib/dwconnect.js](../../postgres/lib/dwconnect.js), [../../postgres/lib/connect.js](../../postgres/lib/connect.js)

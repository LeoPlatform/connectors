# leo-connector-datalake

RStreams connector that reads queues and writes Delta tables in Databricks Unity Catalog.

## Quick start

```sh
cd connectors/datalake
npm install
npm run lint
npm test
```

All unit tests run offline (no credentials needed). Exit 0 is the gate.

## Deferred env config — fill before running test:int / equivalence

The following env vars must be set before running `npm run test:int` or the Step 12 equivalence script. They are blocked on open questions in `docs/BUILD_PLAN.md`.

| Env var | Open question | Notes |
|---|---|---|
| `DATABRICKS_HOST` | #3 | Nonprod workspace hostname (e.g. `adb-xxx.azuredatabricks.net`) |
| `DATABRICKS_HTTP_PATH` | #3 | SQL warehouse HTTP path (e.g. `/sql/1.0/warehouses/abc`) |
| `DATABRICKS_TOKEN` | #3 | Service-principal PAT or OAuth token; comes from AWS Secrets Manager at runtime |
| `DATABRICKS_CATALOG` | #3 | Nonprod Unity Catalog name |
| `DATABRICKS_SCHEMA` | #3 | Per-run scratch schema — set to `datalake_test_<uuid>` before each run; `after()` hook drops it |
| `AWS_S3_BUCKET` | #6 | Staging bucket covered by UC External Location with READ FILES grant |
| `AWS_S3_PREFIX` | #6 | Staging prefix under that External Location |
| `AWS_REGION` | #6 | Bucket region |

Also required before Step 11 (schema evolution):
- **MODIFY grant** (`ALTER TABLE ... ADD COLUMN`) on target catalog/schema — open question #5. Confirm via `infra-iac-databricks/` Terraform before running `schema_evolution.test.js`.

Also required before Step 12 (equivalence):
- **UC External Location + READ FILES** grant on staging prefix for loader service principal — open question #6.
- **Predictive Optimization** on the nonprod catalog (or `optimizeWrite`/`autoCompact` Spark conf) — open question #8.

When the nonprod environment is locked, fill in the allowlists in `test/integration/helpers/databricks.js`:
```js
const NONPROD_HOST_ALLOWLIST = ['your-nonprod-host.azuredatabricks.net'];
const NONPROD_BUCKET_ALLOWLIST = ['your-nonprod-staging-bucket'];
```

## Integration test run

```sh
export DATABRICKS_HOST=...
export DATABRICKS_HTTP_PATH=...
export DATABRICKS_TOKEN=...
export DATABRICKS_CATALOG=...
export DATABRICKS_SCHEMA=datalake_test_$(uuidgen | tr -d '-' | head -c 8 | tr '[:upper:]' '[:lower:]')
export AWS_S3_BUCKET=...
export AWS_S3_PREFIX=...
export AWS_REGION=...
npm run test:int
```

If a run crashes before the `after()` hook runs, clean up manually:
```sql
DROP SCHEMA <catalog>.<schema> CASCADE;
```

## Equivalence check (Step 12 / DoD)

```sh
node test/equivalence/run.js \
  --input <path-to-captured-fixture>.jsonl \
  --tables <comma-separated-coverage-set>
```

Not yet implemented — see `test/equivalence/run.js` for the specification and the open questions blocking it.

## Architecture

See `docs/BUILD_PLAN.md` for the full 12-step build sequence.

Key files:
- `lib/connect.js` — Databricks connection, `escapeId` (lowercase + backtick), `streamToTableFromS3`
- `lib/dwconnect.js` — factory + `importFact` + `changeTableStructure`
- `lib/sql.js` — pure DDL/DML generation (`createTable`, `mergeFact`, type mapping)
- `lib/surrogate_key.js` — FarmFingerprint64 surrogate key (matches Redshift FARMFINGERPRINT64)
- `lib/dol.js` — Domain Object Layer (consumer-side queries, Databricks dialect)

## Identifier casing

All identifiers are lowercased at SQL generation time via `client.escapeId`. This sidesteps Databricks case-preserving / case-insensitive split. All audit of producer repos found zero mixed-case identifiers, so the convention has zero migration cost. See BUILD_PLAN.md open question #7.

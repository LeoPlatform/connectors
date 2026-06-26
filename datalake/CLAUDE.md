# datalake-connector

This project implements a new RStreams connector for writing to a data lake. It begins as a fork of the in-use Redshift connector — which is actually based on leo-connector-postgres, not leo-connector-redshift — as well as leo-connector-common. We use a similar (identical when appropriate) combination of straightforward code and configuration ("dw_fields" JSON) that describes how an RStreams event is transformed and tabulated — meaning how a JSON event is converted into one or more (typically flat) relational records, and how those records will be merged into data lake tables by the loader mechanism - usually with a table having one up-to-date record per relevant entity, not per event.

The goal of this project is to increase data development capacity by facilitating decentralized ownership of data lake tables by domain teams (the data/event producers) rather than by a central data team downstream.

## Business Domain

This connector handles ingestion of retailer platform event data from RStreams queues into Databricks Delta tables. Key concepts:

- **`dw_fields` JSON** — schema definition driving field mapping, types, natural keys (`nk`), surrogate keys (`sk`), and grouping. Stored in DynamoDB `${Stage}DW-Fields`; source of truth is the JSON files in each producer repo.
- **`identifier`** — table name (e.g., `d_order`, `f_order_item`); `d_` prefix = dimension (one row per entity), `f_` prefix = fact (one row per event/activity)
- **`groups`** — queue routing (e.g., `"dim"`, `"quantity"`); determines which loader group processes this table
- **Surrogate keys** — FarmFingerprint64 hashes of the natural key, computed in Node.js via `farmhash-modern` and written into the staging CSV
- **ETL pattern** (transformation before load): RStreams enrichments (change detection e.g. item-old-new → modified-product, record shaping e.g. modified-product → dimension) feed into offload (staging records to S3, mounting as a relation in Databricks, merging into live tables via DML MERGE)

## Architecture & Layout

This is a **library package** within the LeoPlatform/connectors monorepo. It is not a deployed Lambda service — it publishes as an npm package consumed by bot services, e.g. [general](https://github.com/Chub-Engineering/general).

**Projected structure** (code not yet written — matches sibling connector pattern):
```
connectors/datalake/
├── index.js           # Main export — connector class extending leo-connector-common/base
├── package.json       # mocha, farmhash-modern, Databricks SDK
├── lib/
│   ├── connect.js     # Databricks connection: staging CSV → temp view → MERGE INTO Delta
│   ├── checksum.js    # Checksumming (adapted from ../redshift/lib/checksum.js)
│   └── dol.js         # Domain Object Layer — Databricks SQL dialect query builder
├── docs/              # Design docs and principles
│   └── project_principles.md
└── test/
    ├── unit/          # Per lib/ module (mocha + chai)
    └── integration/   # Against Databricks dev workspace
```

**Key upstream reuse from `leo-connector-common`:**
- `datawarehouse/transform.js` — parse enriched event records (change-detected, shaped) into table record per `dw_fields` schema
- `datawarehouse/combine.js` — dedup and sort by natural key before staging to S3
- `datawarehouse/load.js` — orchestrates offload: stage records to S3, mount as relation in target DB, merge into live tables

## Deployment Framework

Library package only — no serverless.yml or CDK here. The bots that consume this connector (e.g., `general/`) deploy via their own Serverless Framework configs. To add the connector as a dependency in a bot during development: use a local path reference in `package.json`.

## Environments

Table name resolved at deploy time via CloudFormation export: `${proper}DW-Fields`, injected as `process.env.DWFields`.

Databricks workspace targets are configured per environment via environment variables (dev / preprod / prod workspaces). Credentials come from AWS Secrets Manager.

## Resources & Infrastructure

| Resource | Details |
|---|---|
| DynamoDB `${Stage}DW-Fields` | Read at runtime for schema definitions; table name via `process.env.DWFields` |
| RStreams queues | `dim`, `item-quantity-dim`, `supplier-catalog-dim` — schedules and limits in `general/bots/multi-offload-redshift/serverless.yml` and `index.js` |
| Databricks Unity Catalog | Write target for Delta tables; replaces Redshift `datawarehouse.public.*`; service principal needs `MODIFY` on each table for schema evolution (`ADD COLUMN`, `ALTER COLUMN`, `DROP COLUMN`) |
| S3 | Staging area for pipe-delimited CSV files before COPY INTO |
| AWS Secrets Manager | Databricks credentials |
| Leo checkpointing (DynamoDB) | RStreams progress tracking; managed by leo-sdk |

## Security Boundaries

- **Never** commit credentials, `.env` files, or Databricks tokens. All secrets via AWS Secrets Manager / SSM.
- **PII check first**: before reading any field value, check the attribute name against known PII fields (`ship_address1`, `ship_address2`, `first_name`, `last_name`, `email`, `phone_*`). If suspected PII, pass schema/metadata only to AI — never the values.
- **No data to the web**: never send Rithum source code or event payload data to external services.

## Exemplar References

The publishing/offload path does **not** go through `leo-connector-redshift`. The bot-side caller (`general/lib/offload_to_redshift.js`) loads `leo-connector-common/datawarehouse/load.js` and `leo-connector-postgres/lib/dwconnect.js` — `connectors/redshift` is only used by the `report/` consumer. Model the data-publishing side on `connectors/postgres`, not `connectors/redshift`.

**Path convention:** `../postgres/`, `../common/`, `../redshift/` are sibling packages in this monorepo (workspace-relative). References to other Rithum repos name the repo and its in-repo path (e.g. the `general` repo's `lib/offload_to_redshift.js`) — they are separate repos, not paths under this checkout.

| What | Where | Why |
|---|---|---|
| **Offload mechanics** (staging, merge, schema discovery, `_auditdate`/`_current`/`_deleted` cols, dim/fact handling) | `../postgres/lib/dwconnect.js` | The actual code `offload_to_redshift.js` invokes; the datalake connector must expose the same shape, staging to S3 + `COPY INTO`/`MERGE` against Delta |
| Pipeline orchestration | `../common/datawarehouse/load.js`, `combine.js`, `transform.js` | Reused unchanged; understand before touching |
| `package.json` / `index.js` shape, file layout | `../postgres/package.json`, `../postgres/index.js` | Drop `pg`/`pg-copy-streams`/`pg-format`; add `farmhash-modern` + Databricks SQL client |
| Connection layer | `../postgres/lib/connect.js` | Connection-pool plumbing; adapt to Databricks SQL client |
| Domain Object Layer (consumer-side queries) | `../postgres/lib/dol.js` | SQL query builder pattern; Databricks-dialect adaptation. Not on the publishing critical path |
| dw_fields schema format | the `order` repo's `dw_fields/d_order.json` | Canonical dimension schema with nk, sk, groups, structure |
| How bots call the connector | the `general` repo's `lib/offload_to_redshift.js` | Calling convention; datalake connector must match this interface |

**Skip:** `../postgres/lib/binlogreader.js`, `../postgres/lib/lsn.js`, `../postgres/lib/test_decoding.js` — CDC/logical-replication; not relevant to RStreams-driven publishing.

### Porting discipline (read before adapting any postgres helper)

When porting a function from `../postgres/`, **preserve every branch** in the original — don't silently flatten conditional logic to "simplify" the Databricks version. The bugs from doing that are exactly the kind that pass unit tests, slip past code review, and corrupt data at runtime months later. Concrete patterns to watch for:

- **Dim vs fact branching.** Many postgres helpers gate behavior on `definition.isDimension` — e.g., `createTable` adds `_deleted` only for facts and `_startdate`/`_enddate`/`_current` only for dims. Mirror the branch; don't unconditionally apply one side's columns.
- **Configuration gates.** Postgres often gates a feature on `config.hashedSurrogateKeys`, `config.version`, etc. If the datalake side only supports one branch today, keep the conditional and `throw` (or no-op) the unsupported branch — don't delete it. Future contributors need to see the shape.
- **Validate with a postgres-side grep, not the unit tests.** The unit-test fixtures in this repo were written alongside the port and can encode the same flattening bug — a passing test proves consistency with the test, not with the postgres source. When in doubt, `grep -n "<helper>" ../postgres/lib/dwconnect.js` and read the surrounding 30–50 lines.

Separately: some review-flagged patterns *should* match postgres and were kept on purpose. Before "improving" something that looks unidiomatic, check [docs/porting_decisions.md](docs/porting_decisions.md) — that's the closed list of intentionally-not-fixed items.

## Coding Rules

**Always:**
- Check attribute names for PII before reading values
- Match sibling connector conventions (plain JavaScript, no TypeScript)
- Map `varchar(n)` → `string` at SQL generation time (Databricks has no length-constrained string type)
- Compute surrogate keys via `farmhash-modern` in Node.js, write computed value into staging CSV — not via SQL function
- Map all Redshift `TIMESTAMP` (no TZ) columns to Databricks `TIMESTAMP_NTZ` and keep the connector's TZ posture unchanged — see [Timestamp handling](#timestamp-handling--preserving-legacy-no-tz-semantics) below
- Keep all dw_fields config changes additive during Redshift coexistence — no change may break the running Redshift pipeline

**Never:**
- Release changes that impact the running Redshift pipeline as side effects. The publishing-path code (`../postgres/` — especially `lib/dwconnect.js` — plus `../common/datawarehouse/`, the `general` repo's `lib/offload_to_redshift.js`, and the offload bots) may be refactored or extended (e.g., to enable shared patterns), but all changes must be backwards-compatible and safe by default — new functionality disabled unless explicitly enabled. `../redshift/` is consumer-only (used by `report/`) and is not on the publishing path. The Redshift pipeline must remain independently deployable and unaffected by what is or isn't complete on the Databricks side.
- Use Redshift-specific SQL syntax in new code (`GETDATE()`, `TOP N`, `DISTKEY`/`SORTKEY` in DDL, `FARMFINGERPRINT64()`)
- Publish to npm or bump the package version without explicit approval from the user — publishing is manual, requires an `leoinsights` org token, and must be preceded by a passing integration test run
- Add npm dependencies without asking first
- Write SQL stored procedures
- Commit credentials or Databricks tokens
- Push code containing local file paths in `package.json` (e.g., `"file:../../connectors/datalake"`); only use npm registry versions for published code. Local paths are for development only; before committing, ensure all module references resolve from npm or a registry.

**Ask before:**
- Adding a new top-level field to dw_fields (coordinate with DPLAT-442 / John Cronin — same field, same DynamoDB record; must be safe for the Redshift loader to silently ignore)
- Changing staging or merge semantics in a way that would require any Redshift-side change to remain safe
- Changing how surrogate keys are computed (output must remain identical to `FARMFINGERPRINT64()`)

**Redshift pipeline independence:**
The Redshift pipeline (`general/`, `offload_to_redshift.js`, `leo-connector-postgres`, `dwconnect.js`) must be mergeable and deployable to production at any time, completely independent of migration progress. Refactors and improvements are welcome; the constraint is safe gating, not no-touch:
- Refactors and shared infrastructure improvements are fine as long as they remain backwards-compatible and don't change Redshift behavior
- New functionality (e.g., new dw_fields fields, new config options) must be disabled by default and explicitly gated — never activate as a side effect
- All dw_fields config changes must be additive and safe for the Redshift loader to ignore unknown fields (it already does)
- The datalake connector is a new package — it does not replace or wrap the Redshift connector; both exist independently
- Datalake-side bots are new Lambda functions alongside (not replacing) the existing Redshift loader bots
- Any code merged to `main` that touches shared config must leave the Redshift pipeline in a valid, deployable state

## Timestamp handling — preserving legacy no-TZ semantics

Every timestamp column this connector writes — audit columns (`_auditdate`, SCD2 `_startdate`/`_enddate`) and data-payload columns alike — inherits from a Redshift `TIMESTAMP` (no TZ) schema. These columns store a naked wall-clock; the intended timezone is a per-source convention that lives outside the schema:

- DSCO/CUP order timestamps (e.g. `d_order.created_at`) are stored as US/Pacific — pre-converted upstream by `fixTimestamp()` in `@chub-engineering/layer-util` (`DEFAULT_DESTINATION_TIMEZONE = 'US/Pacific'`).
- `f_item_change_event.last_at` is stored as UTC (source string is ISO `Z`; Redshift `COPY TIMEFORMAT 'auto'` strips the offset and stores the wall-clock).
- OrderStream-origin timestamps land as Eastern.
- Audit columns are UTC (Redshift session TZ is UTC, so `sysdate` writes UTC). Verified against ProdDW: `pg_user.useconfig` shows no per-user TZ override — only `looker` and `etl` carry `search_path=…` overrides, nothing TZ-related — so the session UTC assumption holds for every loader user.

The migration must reproduce these wall-clocks bit-for-bit in Databricks during the long coexistence period — consumers cut over per-source over many months, and any shift in the meaning of stored values would silently corrupt every reader. Semantic normalization is a downstream concern (enterprise dbt models, per-consumer queries), not an ingest concern.

**Target type:** `TIMESTAMP_NTZ` — the only Databricks type that preserves "no offset stored, no conversion on read/write."

**Required session parameters** (already set in [lib/connect.js](lib/connect.js#L58-L62)):
- `timezone = UTC` — keeps server-side `current_timestamp()` returning UTC, matching Redshift's UTC-session `sysdate` for audit columns.
- `infer_timestamp_ntz_type = true` — keeps CSV/Parquet timestamp inference from shifting wall-clocks into Databricks' default zone-aware `TIMESTAMP`.

**CSV staging implication:** `read_files` with NTZ inference in PERMISSIVE mode rejects offset markers — empirically confirmed by [`test/integration/timezone.test.js`](test/integration/timezone.test.js): a `Z`-suffixed or `±HH:MM`-suffixed value seen directly by `read_files` nulls out. Any timestamp value written to staging CSV must reach the file as naked ISO local form (`YYYY-MM-DDTHH:MM:SS`), no `Z`, no `±HH:MM`. The connector enforces this in two places:

- [`setAuditdate`](lib/connect.js#L206-L211) — strips the trailing `Z` from `Date.toISOString()` when building the audit timestamp literal.
- [`doStreamToTableFromS3`](lib/connect.js) — for every column whose `columnDef.type` is `TIMESTAMP_NTZ`, routes the value through `stripTimestampOffset()` before the CSV write, normalizing payload values from producers that emit `Date.toISOString()` (always `Z`-suffixed) or any other ISO-with-offset shape.

This is one rule with two implementations; do not treat the audit-column strip as an audit-specific quirk.

The shared audit-timestamp helper [`lib/audit_timestamp.js`](lib/audit_timestamp.js) is the single source of truth for the audit-literal shape. It emits `YYYY-MM-DDTHH:MM:SS` — second-resolution, no millis, no `Z` — matching the postgres/redshift sibling convention exactly except for the unavoidable Z-strip (postgres emits `…SSZ`; see [`connectors/postgres/lib/connect.js`](../postgres/lib/connect.js) `setAuditdate`).

**Do not "fix" this:**
- Don't retype a `TIMESTAMP_NTZ` column to zone-aware `TIMESTAMP` "for clarity" — it forces a UTC interpretation onto values that may not be UTC; Pacific `d_order` values would shift by 7–8 hours and corrupt every downstream join. (Note: this is about retyping existing no-TZ columns. If a producer column is `timestamptz` at the source, mapping it to Databricks `TIMESTAMP` is correct and that's what [`lib/sql.js`](lib/sql.js) does — the offset is in the data and carries its own meaning.)
- Don't apply `CONVERT_TIMEZONE` / `from_utc_timestamp` in the connector — semantic normalization belongs in downstream models, not at ingest.
- Don't change session `timezone` away from UTC — both the `Z` strip and the audit-column UTC assumption depend on it. If you must change it, audit-column writes and the strip have to change together.

**Canonical reference for stored timezones per table:** the `data-warehouse` repo's `docs/timezone-data-conventions.md`.

## Definition of Done

Run in this order; all must pass before considering work complete:
1. `npm run lint` — eslint
2. `npm test` — mocha unit tests
3. `npm run test:int-local` — integration tests against the dev Databricks workspace (requires `DATABRICKS_CONFIG_PROFILE` pointing to a `~/.databrickscfg` section with `host` + auth, or equivalent env vars); uses the `public_stage_local` isolation schema via `LEO_LOCAL=true`

Note: this connector is plain JavaScript (matches all siblings in `connectors/`). There is no TypeScript compilation step.

Additionally:
- New dw_fields config keys must be additive only (no breaking changes to Redshift consumers)
- Connector output verified equivalent to the Redshift connector on the same input events

## Project Principles

See [docs/project_principles.md](docs/project_principles.md).

# datalake-connector

This project implements a new RStreams connector for writing to a data lake. It begins as a fork of the in-use Redshift connector — which is actually based on leo-connector-postgres, not leo-connector-redshift — as well as leo-connector-common. We use a similar (identical when appropriate) combination of straightforward code and configuration ("dw_fields" JSON) that describes how an RStreams event is transformed and tabulated — meaning how a JSON event is converted into one or more (typically flat) relational records, and how those records will be merged into data lake tables by the loader mechanism - usually with a table having one up-to-date record per relevant entity, not per event.

The goal of this project is to increase data development capacity by facilitating decentralized ownership of data lake tables by domain teams (the data/event producers) rather than by a central data team downstream.

## Business Domain

This connector handles ingestion of retailer platform event data from RStreams queues into Databricks Delta tables. Key concepts:

- **`dw_fields` JSON** — schema definition driving field mapping, types, natural keys (`nk`), surrogate keys (`sk`), and grouping. Stored in DynamoDB `${Stage}DW-Fields`; source of truth is the JSON files in each producer repo.
- **`identifier`** — table name (e.g., `d_order`, `f_order_item`); `d_` prefix = dimension (one row per entity), `f_` prefix = fact (one row per event/activity)
- **`groups`** — queue routing (e.g., `"dim"`, `"quantity"`); determines which loader group processes this table
- **`clusterKey`** — optional field for Databricks physical layout hint (`CLUSTER BY`); no effect on Redshift by default (see Coding Rules)
- **Surrogate keys** — FarmFingerprint64 hashes of the natural key, computed in Node.js via `farmhash-modern` and written into the staging CSV
- **Three pipeline stages** (each driven by configuration, not SQL): Ingest (event JSON → record), Tabulate (dedup + key prep), Merge (MERGE INTO Delta table)

## Architecture & Layout

This is a **library package** within the LeoPlatform/connectors monorepo. It is not a deployed Lambda service — it publishes as an npm package consumed by bot services (e.g., `../../general/`).

**Projected structure** (code not yet written — matches sibling connector pattern):
```
connectors/datalake/
├── index.ts           # Main export — connector class extending leo-connector-common/base
├── package.json       # TypeScript, mocha, farmhash-modern, Databricks SDK
├── tsconfig.json      # Strict TypeScript
├── lib/
│   ├── connect.ts     # Databricks connection: staging CSV → temp view → MERGE INTO Delta
│   ├── checksum.ts    # Checksumming (adapted from ../redshift/lib/checksum.js)
│   └── dol.ts         # Domain Object Layer — Databricks SQL dialect query builder
├── docs/              # Design docs and principles
│   └── project-principles.md
└── test/
    ├── unit/          # Per lib/ module (mocha + chai)
    └── integration/   # Against Databricks dev workspace
```

**Key upstream reuse from `leo-connector-common`:**
- `datawarehouse/combine.js` — dedup and sort by natural key before staging
- `datawarehouse/transform.js` — parse event JSON into table record
- `datawarehouse/load.js` — orchestrates the full Ingest → Tabulate → Merge pipeline

## Deployment Framework

Library package only — no serverless.yml or CDK here. The bots that consume this connector (e.g., `general/`) deploy via their own Serverless Framework configs. To add the connector as a dependency in a bot during development: use a local path reference in `package.json`.

## Environments

| Environment | DW-Fields DynamoDB table | Notes |
|---|---|---|
| Test | `TestDW-Fields-1H6FW657GOW3E` | |
| Staging | `StagingDW-Fields-1EH4E5QRRYJ6U` | |
| Production | `ProdDW-Fields-1B5OET8S6WRUF` | |

Databricks workspace targets are configured per environment via environment variables (dev / preprod / prod workspaces). Credentials come from AWS Secrets Manager.

## Resources & Infrastructure

| Resource | Details |
|---|---|
| DynamoDB `${Stage}DW-Fields` | Read at runtime for schema definitions; table name via `process.env.DWFields` |
| RStreams queues | `dim` (every 10 min, 2M events), `item-quantity-dim` (every 5 min), `supplier-catalog-dim` (every 10 min offset) |
| Databricks Unity Catalog | Write target for Delta tables; replaces Redshift `datawarehouse.public.*` |
| S3 | Staging area for pipe-delimited CSV files before COPY INTO |
| AWS Secrets Manager | Databricks credentials |
| Leo checkpointing (DynamoDB) | RStreams progress tracking; managed by leo-sdk |

## Security Boundaries

- **Never** commit credentials, `.env` files, or Databricks tokens. All secrets via AWS Secrets Manager / SSM.
- **PII check first**: before reading any field value, check the attribute name against known PII fields (`ship_address1`, `ship_address2`, `first_name`, `last_name`, `email`, `phone_*`). If suspected PII, pass schema/metadata only to AI — never the values.
- **No data to the web**: never send Rithum source code or event payload data to external services.

## Exemplar References

| What | Where | Why |
|---|---|---|
| Connection layer to adapt | `../redshift/lib/connect.js` | Postgres pool → Databricks client; same interface contract |
| Domain Object Layer | `../redshift/lib/dol.js` | SQL query builder pattern to replicate for Databricks dialect |
| Pipeline orchestration | `../common/datawarehouse/load.js`, `combine.js`, `transform.js` | Reused unchanged; understand before touching |
| dw_fields schema format | `../../order/dw_fields/d_order.json` | Canonical dimension schema with nk, sk, groups, structure |
| How bots call the connector | `../../general/lib/offload_to_redshift.js` | Calling convention; datalake connector must match this interface |

## Coding Rules

**Always:**
- Check attribute names for PII before reading values
- Use TypeScript strict mode; no `any`
- Map `varchar(n)` → `string` at SQL generation time (Databricks has no length-constrained string type)
- Compute surrogate keys via `farmhash-modern` in Node.js, write computed value into staging CSV — not via SQL function
- Use `TIMESTAMP_NTZ` for timezone-naive timestamp columns; set `infer_timestamp_ntz_type = true` on connectors
- Keep all dw_fields config changes additive during Redshift coexistence — no change may break the running Redshift pipeline

**Never:**
- Modify any Redshift-side code as part of datalake connector work — this means no changes to `../redshift/`, `../postgres/`, `../../general/lib/offload_to_redshift.js`, or any existing bot entry point. The Redshift pipeline must remain independently deployable at any point during the migration, unaffected by what is or isn't complete on the Databricks side.
- Use Redshift-specific SQL syntax in new code (`GETDATE()`, `TOP N`, `DISTKEY`/`SORTKEY` in DDL, `FARMFINGERPRINT64()`)
- Change Redshift SORTKEY behavior — Redshift continues inferring sort key from `public.v_dist_sort_key` at runtime; `clusterKey` must have no effect on Redshift by default (see below)
- Add npm dependencies without asking first
- Write SQL stored procedures
- Commit credentials or Databricks tokens

**Ask before:**
- Adding a new top-level field to dw_fields (coordinate with DPLAT-442 / John Cronin — same field, same DynamoDB record; must be safe for the Redshift loader to silently ignore)
- Enabling `clusterKey` as a Redshift SORTKEY override (disabled by default; requires explicit decision)
- Changing staging or merge semantics in a way that would require any Redshift-side change to remain safe
- Changing how surrogate keys are computed (output must remain identical to `FARMFINGERPRINT64()`)

**Redshift pipeline independence:**
The Redshift pipeline (`general/`, `offload_to_redshift.js`, `leo-connector-postgres`, `dwconnect.js`) must be mergeable and deployable to production at any time, completely independent of migration progress. This means:
- All dw_fields config changes must be additive and safe for the Redshift loader to ignore unknown fields (it already does)
- The datalake connector is a new package — it does not replace or wrap the Redshift connector; both exist independently
- Datalake-side bots are new Lambda functions alongside (not replacing) the existing Redshift loader bots
- Any code merged to `main` that touches shared config must leave the Redshift pipeline in a valid, deployable state

**`clusterKey` field — Databricks only by default:**
`clusterKey` in dw_fields drives Databricks `CLUSTER BY` for liquid clustering. The Redshift connector ignores it entirely (unknown fields are silently ignored by the DynamoDB scan). The option to also use it as a Redshift SORTKEY override may be implemented but **must be off by default** and gated behind an explicit configuration flag — never activated as a side effect of adding the field.

## Definition of Done

Run in this order; all must pass before considering work complete:
1. `npm run format` — prettier
2. `npm run type-check` — `tsc --noEmit`, strict mode
3. `npm run lint` — eslint
4. `npm test` — mocha unit tests

Additionally:
- New dw_fields config keys must be additive only (no breaking changes to Redshift consumers)
- Connector output verified equivalent to the Redshift connector on the same input events

## Project Principles (highest priority)

Read and follow [docs/project-principles.md](docs/project-principles.md). These take precedence over the development first principles and platform principles when they overlap. If the user tells you to write any change to principles, write it there.

## Development First Principles (second priority)

Read `docs/development-first-principles.md` from the workspace root (`~/git/dw/docs/development-first-principles.md`). Covers software design, security, code quality, and AI-assisted development standards.

## Data Platform Principles (third priority)

When doing design, architecture, spec, or implementation work, also read `docs/data-platform-principles-and-strategies.md` from the workspace root (`~/git/dw/docs/data-platform-principles-and-strategies.md`).

## Execution Overview

[Project docs on Notion](https://app.notion.com/p/commercehub/Proposal-Migrate-Retailer-data-warehouse-to-Databricks-344e0f2aafae801990b7c88822458a0b) — when adding or moving docs to Notion, complete these steps:
1. Create the new page as a child of the Proposal page
2. Fetch the Proposal page content to find the "# Project Docs" section
3. Add a link to the newly created page under Project Docs (before the "---" separator that precedes the Reference section)
4. Update the Proposal page with the modified content

High-level project plans live at [EDW Migration](https://www.notion.so/commercehub/EDW-Migration-Redshift-to-Databrcks-347e0f2aafae80aca55ff27de210ea26?source=copy_link)

The technical design of the lift-and-shift work is in [Technical Overview: Lift, Shift, and Rebuild](https://www.notion.so/commercehub/Technical-Overview-Lift-Shift-and-Rebuild-34fe0f2aafae81e1876efe7bb3189c26?source=copy_link).

The PII decision space for migrating the Redshift `public` schema is in [PII Handling: Options for Migrating Redshift public to the Data Lake](https://www.notion.so/commercehub/PII-Handling-Options-for-Migrating-Redshift-public-to-the-Data-Lake-34fe0f2aafae81969264e601c6e041ed?source=copy_link).

Resolved decisions on dw_fields and dw-schema configuration for Databricks coexistence (type mapping, physical hints, varchar validation, surrogate key computation) are in [Configuration Migration Analysis](https://app.notion.com/p/351e0f2aafae8128b7baed05f6a5adbc).

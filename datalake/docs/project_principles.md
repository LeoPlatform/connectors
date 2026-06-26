# datalake-connector Project Principles

## P0. Target-Agnostic by Design

This connector is named `leo-connector-datalake`, not `leo-connector-databricks`. The intent is a connector that works against any data lake target, with Databricks Unity Catalog as the initial implementation. If a different target is needed (e.g. a different lakehouse platform or storage format), it should be added as a configured variant within this package — not as a separate connector. The configuration-driven ingestion model (P1) is what makes this possible: the three pipeline stages are defined by `dw_fields` config, and the target-specific SQL dialect and staging mechanics are isolated in `lib/connect.js` and `lib/sql.js`.

## P1. Configuration-Driven Ingestion
In this project we will retain the general approach taken in the [legacy architecture](https://app.notion.com/p/352e0f2aafae81a0ba18e26700646506), and adapt it from a Redshift data sink to a Databricks data sink. Changes may be proposed but we do essentially want to enable a lift-and-shift of the dsco datawarehouse database (public schema) ingestion process from Redshift to Databricks. 

The three core transformation stages of the ingestion pipeline shall each be defined by configuration, not SQL:

1. **Ingest** — transforming event JSON into a semi-structured table record
2. **Tabulate** — transforming a semi-structured table record into modified-entity records
3. **Merge** — merging modified-entity records into an existing table

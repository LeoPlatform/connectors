# datalake-connector Project Principles

> These principles are specific to the datalake-connector project and take precedence over the general [Data Platform Principles and Strategies](data-platform-principles-and-strategies.md) when they overlap.

## P1. Configuration-Driven Ingestion
In this project we will retain the general approach taken in the [legacy architecture](https://app.notion.com/p/352e0f2aafae81a0ba18e26700646506), and adapt it from a Redshift data sink to a Databricks data sink. Changes may be proposed but we do essentially want to enable a lift-and-shift of the dsco datawarehouse database (public schema) ingestion process from Redshift to Databricks. 

The three core transformation stages of the ingestion pipeline shall each be defined by configuration, not SQL:

1. **Ingest** — transforming event JSON into a semi-structured table record
2. **Tabulate** — transforming a semi-structured table record into modified-entity records
3. **Merge** — merging modified-entity records into an existing table

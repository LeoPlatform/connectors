# Datalake Connector — Follow-up Roadmap

## Dependency upgrades (deferred until real-data validation)

### async 2.6.3 → 3.x

The v3 callback API is documented as identical to v2 (`series`, `parallelLimit` signatures unchanged), so no code changes are expected. Held because the full write path should be exercised with real production data before touching runtime deps.

### fast-csv 2.4.1 → 5.x

Requires changing `csv.createWriteStream()` → `csv.format()` in `lib/connect.js` and the `csvRowsFromObjects` helper in `test/unit/importFact.test.js`. The production write path is mocked in unit tests and only exercised end-to-end in integration tests — real data is needed to catch CSV formatting edge cases before upgrading.

Do async first (lower-risk, standalone), then fast-csv.

## Parameter binding for delete IN (...)

Feedback on PR [#235](https://github.com/LeoPlatform/connectors/pull/235) recommends parameterized `IN` binds, which removes the dependency on `ansi_mode=false` backslash-escape semantics entirely.

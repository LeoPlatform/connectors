#!/usr/bin/env node
'use strict';

// Step 12 — Equivalence script (DoD check)
// Deferred: requires a captured prod fixture + both nonprod environments live.
// Blocked on open questions #3 and #6 in BUILD_PLAN.md.
//
// Usage (when env is ready):
//   node test/equivalence/run.js \
//     --input <path-to-captured-fixture>.jsonl \
//     --tables <comma-separated-coverage-set>
//
// Coverage set requirements (from BUILD_PLAN.md Step 12):
//   - ≥1 d_* and ≥1 f_* table
//   - Type coverage: varchar→STRING, timestamp→TIMESTAMP_NTZ, int/bigint, boolean
//   - ≥1 fact with clusterKey set (exercises naturalKeyFilter MERGE pruning path)
//   - Mix of single-column and composite natural keys
//   - Must be a subset of tables the captured dim-queue fixture actually populates
//
// The script:
//   (a) Lakebridge reconciliation — see docs/migration-tools.md for invocation
//   (b) Hand-rolled MD5 row-level diff:
//       - Loads fixture through BOTH the Redshift loader and this datalake loader
//         (both targeting nonprod — see Live-write rule in BUILD_PLAN.md)
//       - Compares SELECT <nk>, MD5(CONCAT_WS('|', col1, col2, ...)) row-by-row
//       - Prints actionable diffs on divergence; exits 0 on zero diffs

console.error([
	'',
	'ERROR: Equivalence script is not yet implemented.',
	'',
	'Blocked on:',
	'  #3 — nonprod Databricks workspace (host, path, token, catalog, schema)',
	'  #6 — UC External Location + READ FILES grant on staging S3 bucket/prefix',
	'',
	'Steps to implement when env is ready:',
	'  1. Lock the coverage set (see requirements in this file header)',
	'  2. Capture a 1k-event prod batch from the dim queue (PII-scrubbed)',
	'  3. Implement the MD5 comparison loop against both loaders',
	'  4. Document chosen coverage set + nonprod env names in README.md',
	'',
	'See BUILD_PLAN.md Step 12 for full specification.',
].join('\n'));

process.exit(1);

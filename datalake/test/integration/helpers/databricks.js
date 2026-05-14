'use strict';

// Integration test helper — reads env vars for Databricks + S3 targets.
// All env vars are listed in connectors/datalake/README.md § "Deferred env config".
// If any required var is unset, mocha `this.skip()` is called so the suite
// exits 0 offline without failing CI.
//
// Open questions blocking integration tests:
//   #3 — nonprod Databricks workspace hostname, HTTP path, token, catalog, schema
//   #6 — UC External Location + READ FILES grant on staging S3 bucket/prefix

const REQUIRED_ENV = [
	'DATABRICKS_HOST',
	'DATABRICKS_HTTP_PATH',
	'DATABRICKS_TOKEN',
	'DATABRICKS_CATALOG',
	'AWS_S3_BUCKET',
	'AWS_S3_PREFIX',
];

// Nonprod safety guard: refuse to run against prod resources.
// Allowlist is intentionally empty until nonprod env names are locked per #3.
// Fill this in when the nonprod environment is confirmed.
const NONPROD_HOST_ALLOWLIST = [
	// e.g. 'nonprod.azuredatabricks.net',
	// PLACEHOLDER — open question #3
];

const NONPROD_BUCKET_ALLOWLIST = [
	// e.g. 'rithum-datalake-nonprod-staging',
	// PLACEHOLDER — open question #6
];

function checkNonprod(host, bucket) {
	if (NONPROD_HOST_ALLOWLIST.length && !NONPROD_HOST_ALLOWLIST.some(h => host.includes(h))) {
		throw new Error(`SAFETY: DATABRICKS_HOST "${host}" is not in nonprod allowlist. Update NONPROD_HOST_ALLOWLIST in test/integration/helpers/databricks.js when nonprod env is locked (#3).`);
	}
	if (NONPROD_BUCKET_ALLOWLIST.length && !NONPROD_BUCKET_ALLOWLIST.some(b => bucket.includes(b))) {
		throw new Error(`SAFETY: AWS_S3_BUCKET "${bucket}" is not in nonprod allowlist. Update NONPROD_BUCKET_ALLOWLIST in test/integration/helpers/databricks.js when nonprod env is locked (#6).`);
	}
}

function getConfig() {
	const missing = REQUIRED_ENV.filter(v => !process.env[v]);
	if (missing.length) {
		return null; // caller should this.skip()
	}
	return {
		host: process.env.DATABRICKS_HOST,
		path: process.env.DATABRICKS_HTTP_PATH,
		token: process.env.DATABRICKS_TOKEN,
		catalog: process.env.DATABRICKS_CATALOG,
		schema: process.env.DATABRICKS_SCHEMA, // per-run UUID schema set by test runner
		s3Bucket: process.env.AWS_S3_BUCKET,
		s3prefix: process.env.AWS_S3_PREFIX,
		region: process.env.AWS_REGION,
	};
}

module.exports = { getConfig, checkNonprod, REQUIRED_ENV };

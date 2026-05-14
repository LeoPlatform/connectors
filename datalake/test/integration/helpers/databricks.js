'use strict';

// Integration test helper — reads env vars for Databricks connection.
// All env vars are listed in connectors/datalake/README.md § "Integration tests".
// If any required var is unset, mocha `this.skip()` is called so the suite
// exits 0 offline without failing CI.
//
// S3 bucket and staging prefix are derived from Unity Catalog RootLocation at
// runtime — they are not injected via env vars.
//
// Open questions blocking integration tests:
//   #3 — SQL warehouse HTTP path, Databricks token/SP credentials
//   #5 — MODIFY grant on target catalog/schema
//   #6 — UC External Location READ FILES grant on staging S3 prefix

const REQUIRED_ENV = [
	'DATABRICKS_HOST',
	'DATABRICKS_HTTP_PATH',
	'DATABRICKS_TOKEN',
	'DATABRICKS_CATALOG',
	'DATABRICKS_SCHEMA',
	'AWS_REGION',
];

// Nonprod safety guard: refuse to run against prod resources.
const NONPROD_HOST_ALLOWLIST = [
	'dbc-0b0acbc9-467a.cloud.databricks.com',
];

function checkNonprod(host) {
	if (!NONPROD_HOST_ALLOWLIST.some(h => host.includes(h))) {
		throw new Error(`SAFETY: DATABRICKS_HOST "${host}" is not in nonprod allowlist. Refusing to run integration tests against a non-dev workspace.`);
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
		schema: process.env.DATABRICKS_SCHEMA,
		region: process.env.AWS_REGION,
	};
}

module.exports = { getConfig, checkNonprod, REQUIRED_ENV };

'use strict';

// Integration test helper — profile-first config resolver.
//
// Resolution order per field:
//   1. Environment variable (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, etc.) — explicit override
//   2. ~/.databrickscfg [<profile>] — default `dev-cup`, override via DATABRICKS_CONFIG_PROFILE
//   3. Locked defaults below (catalog, schema, warehouse HTTP path, region, S3 bucket/prefix)
//
// If neither env nor profile yields host + auth (token OR client_id/client_secret),
// `getConfig()` returns null and the caller should `this.skip()` so `npm run test:int`
// stays green offline.
//
// AWS credentials for S3 staging come from the standard chain (~/.aws, SSO, env).
// Cross-account write to the DE bucket is granted by the bucket policy attached in
// data-lake-infrastructure/src/data_lake/dsco_cross_account_stack.py
// (_create_dsco_developer_resource_policy) — no per-developer assume-role needed.

const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_PROFILE = 'dev-cup';

// Locked defaults — BUILD_PLAN.md §"Open questions" #3 (resolved 2026-05-26).
// s3Bucket = `datalake-dev-641864320185-us-east-1` — the main data-lake bucket the
// existing offload bot writes to (data-lake-ingestion-bots/serverless.yml) and the
// catalog's `datalake-dev-external-location` covers. NOT `datalake-stage-dev-...`
// which is a separate Databricks-only staging bucket.
//
// s3Prefix follows BUILD_PLAN.md §6: per-schema under `stage/data/internal/rithum/`.
const DEFAULTS = {
	path: '/sql/1.0/warehouses/5d84579f11466e3f',
	catalog: 'de_cup_dev_us',
	schema: 'public_stage_local',
	region: 'us-east-1',
	s3Bucket: 'datalake-dev-641864320185-us-east-1',
	s3Prefix: 'stage/data/internal/rithum/public_stage_local',
};

// Nonprod safety guard: refuse to run against prod resources.
const NONPROD_HOST_ALLOWLIST = [
	'dbc-0b0acbc9-467a.cloud.databricks.com',
];

function checkNonprod(host) {
	if (!NONPROD_HOST_ALLOWLIST.some(h => host.includes(h))) {
		throw new Error(`SAFETY: DATABRICKS_HOST "${host}" is not in nonprod allowlist. Refusing to run integration tests against a non-dev workspace.`);
	}
}

// Minimal .ini parser — only what .databrickscfg needs (sections + key=value).
function parseIni(text) {
	const sections = {};
	let current = null;
	for (const rawLine of text.split('\n')) {
		const line = rawLine.replace(/[;#].*$/, '').trim();
		if (!line) continue;
		const sectionMatch = line.match(/^\[(.+)\]$/);
		if (sectionMatch) {
			current = sectionMatch[1].trim();
			sections[current] = sections[current] || {};
			continue;
		}
		if (!current) continue;
		const eq = line.indexOf('=');
		if (eq < 0) continue;
		const key = line.slice(0, eq).trim();
		const value = line.slice(eq + 1).trim();
		sections[current][key] = value;
	}
	return sections;
}

function loadProfile(profileName) {
	const cfgPath = path.join(os.homedir(), '.databrickscfg');
	if (!fs.existsSync(cfgPath)) return null;
	const sections = parseIni(fs.readFileSync(cfgPath, 'utf8'));
	return sections[profileName] || null;
}

function stripScheme(host) {
	if (!host) return host;
	return host.replace(/^https?:\/\//, '').replace(/\/+$/, '');
}

function getConfig() {
	const profileName = process.env.DATABRICKS_CONFIG_PROFILE || DEFAULT_PROFILE;
	const profile = loadProfile(profileName) || {};

	const host = stripScheme(process.env.DATABRICKS_HOST || profile.host);
	const token = process.env.DATABRICKS_TOKEN || profile.token;
	const clientId = process.env.DATABRICKS_CLIENT_ID || profile.client_id;
	const clientSecret = process.env.DATABRICKS_CLIENT_SECRET || profile.client_secret;

	const hasAuth = !!(token || (clientId && clientSecret));
	if (!host || !hasAuth) {
		return null; // caller should this.skip()
	}

	return {
		host,
		path: process.env.DATABRICKS_HTTP_PATH || DEFAULTS.path,
		token: token || undefined,
		clientId: clientId || undefined,
		clientSecret: clientSecret || undefined,
		catalog: process.env.DATABRICKS_CATALOG || DEFAULTS.catalog,
		schema: process.env.DATABRICKS_SCHEMA || DEFAULTS.schema,
		region: process.env.AWS_REGION || DEFAULTS.region,
		s3Bucket: process.env.DATALAKE_S3_BUCKET || DEFAULTS.s3Bucket,
		s3Prefix: process.env.DATALAKE_S3_PREFIX || DEFAULTS.s3Prefix,
		profileName,
	};
}

module.exports = { getConfig, checkNonprod, DEFAULTS };

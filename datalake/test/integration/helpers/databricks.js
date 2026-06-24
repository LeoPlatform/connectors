'use strict';

// Integration test helper — profile-first config resolver.
//
// Resolution order per field:
//   1. Environment variable (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, etc.) — explicit override
//   2. ~/.databrickscfg [<profile>] — default `dev-cup`, override via DATABRICKS_CONFIG_PROFILE
//   3. Per-workspace defaults auto-selected by host (see DEFAULTS_BY_HOST below)
//
// Local developer isolation: set LEO_LOCAL=true to use the `public_stage_local` schema
// and matching S3 prefix instead of the shared `public_stage`. Follows the same leo-config
// convention bots use. Individual env vars still override any field.
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

// Per-environment defaults keyed by host fragment.
// s3Prefix follows BUILD_PLAN.md §6: per-schema under `stage/data/internal/rithum/`.
// Both workspaces default to the shared `public_stage` schema. Set LEO_LOCAL=true to use
// the `public_stage_local` isolation schema instead (same leo-config convention bots use).
// Catalog naming: `de_cup_{env}_us` convention.
// All fields are still overridable by env var (see getConfig()).
const DEFAULTS_BY_HOST = {
	'dbc-0b0acbc9-467a': { // dev workspace (Dsco test)
		path: '/sql/1.0/warehouses/5d84579f11466e3f',
		catalog: 'de_cup_dev_us',
		schema: 'public_stage',
		region: 'us-east-1',
		s3Bucket: 'datalake-dev-641864320185-us-east-1',
		s3Prefix: 'stage/data/internal/rithum/public_stage',
	},
	'dbc-903fa4be-915e': { // preprod workspace (Dsco staging)
		path: '/sql/1.0/warehouses/767c382814eb7b30',
		catalog: 'de_cup_preprod_us',
		schema: 'public_stage',
		region: 'us-east-1',
		s3Bucket: 'datalake-preprod-641864320185-us-east-1',
		s3Prefix: 'stage/data/internal/rithum/public_stage',
	},
};
const DEV_DEFAULTS = DEFAULTS_BY_HOST['dbc-0b0acbc9-467a'];

// Safety guard: refuse to run against prod. Only dev and preprod are allowed.
const ALLOWED_HOSTS = [
	'dbc-0b0acbc9-467a.cloud.databricks.com', // dev
	'dbc-903fa4be-915e.cloud.databricks.com', // preprod
];

function checkAllowedHost(host) {
	if (!ALLOWED_HOSTS.some(h => host.includes(h))) {
		throw new Error(`SAFETY: DATABRICKS_HOST "${host}" is not in the allowed-host list. Refusing to run integration tests against this workspace (prod is blocked).`);
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

	const envKey = Object.keys(DEFAULTS_BY_HOST).find(k => host.includes(k));
	const envDefaults = DEFAULTS_BY_HOST[envKey] || DEV_DEFAULTS;
	const isLocal = process.env.LEO_LOCAL === 'true';
	const defaultSchema = isLocal ? envDefaults.schema + '_local' : envDefaults.schema;
	const defaultS3Prefix = isLocal ? envDefaults.s3Prefix + '_local' : envDefaults.s3Prefix;

	const region = process.env.AWS_REGION || envDefaults.region;

	return {
		host,
		path: process.env.DATABRICKS_HTTP_PATH || envDefaults.path,
		token: token || undefined,
		clientId: clientId || undefined,
		clientSecret: clientSecret || undefined,
		catalog: process.env.DATABRICKS_CATALOG || envDefaults.catalog,
		schema: process.env.DATABRICKS_SCHEMA || defaultSchema,
		region,
		s3Bucket: process.env.DATALAKE_S3_BUCKET || envDefaults.s3Bucket,
		s3Prefix: process.env.DATALAKE_S3_PREFIX || defaultS3Prefix,
		profileName,
	};
}

module.exports = { getConfig, checkAllowedHost, ALLOWED_HOSTS, DEFAULTS_BY_HOST };

'use strict';

// Step 9 — Integration harness
// Verifies connectivity, schema accessibility, and staging-location resolution.
// Throws (fails) when DATABRICKS_CONFIG_PROFILE + ~/.databrickscfg or DATABRICKS_HOST+auth env vars are not configured.
//
// Uses fixed schema names (public_stage_local for local dev, public_stage for CI);
// isolation is per-branch catalog, not per-run UUID schema.

const { expect } = require('chai');
const { getConfig, checkAllowedHost } = require('./helpers/databricks.js');

let dbconfig;
let client;

before(function() {
	dbconfig = getConfig();
	checkAllowedHost(dbconfig.host);
});

describe('Integration harness', function() {
	this.timeout(60000);

	before(function() {
		const connect = require('../../lib/connect.js');
		client = connect(dbconfig);
	});

	it('can execute a query', async function() {
		await new Promise((resolve, reject) => {
			client.query('SELECT 1 AS n', [], (err, rows) => {
				if (err) return reject(err);
				expect(rows[0].n).to.equal(1);
				resolve();
			});
		});
	});

	it('target schema exists in information_schema', async function() {
		await new Promise((resolve, reject) => {
			client.query(
				`SELECT schema_name FROM ${dbconfig.catalog}.information_schema.schemata WHERE schema_name = ?`,
				[dbconfig.schema],
				(err, rows) => {
					if (err) return reject(err);
					expect(rows.map(r => r.schema_name), `schema "${dbconfig.schema}" not found in catalog "${dbconfig.catalog}"`).to.include(dbconfig.schema);
					resolve();
				}
			);
		});
	});

	it('staging location resolves to a valid s3:// URI', function() {
		// Exercises the staging-location resolution contract that streamToTableFromS3 relies on.
		// In local dev the helper supplies explicit s3Bucket/s3Prefix; in environments where
		// the schema has a managed RootLocation, connect.js can fall back to UC lookup.
		expect(dbconfig.s3Bucket, 's3Bucket must be set in config').to.be.a('string').and.not.empty;
		expect(dbconfig.s3Prefix, 's3Prefix must be set in config').to.be.a('string').and.not.empty;
		const uri = `s3://${dbconfig.s3Bucket}/${dbconfig.s3Prefix}`;
		expect(uri).to.match(/^s3:\/\/[^/]+\/.+$/);
	});
});

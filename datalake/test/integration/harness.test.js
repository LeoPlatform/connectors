'use strict';

// Step 9 — Integration harness
// Verifies that the per-run schema is created and dropped correctly.
// Skips all tests when required env vars are unset (exit 0 offline).
//
// Deferred: blocked on open questions #3 and #6 in BUILD_PLAN.md.
// See connectors/datalake/README.md § "Deferred env config" for what to fill in.

const { getConfig, checkNonprod } = require('./helpers/databricks.js');

let dbconfig;
let client;
let runSchema;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) {
		return this.skip();
	}
	checkNonprod(dbconfig.host, dbconfig.s3Bucket);

	// Per-run UUID schema is expected to be set by the caller via DATABRICKS_SCHEMA
	// e.g.: export DATABRICKS_SCHEMA=datalake_test_$(uuidgen | tr -d - | head -c 8 | tr '[:upper:]' '[:lower:]')
	runSchema = dbconfig.schema;
	if (!runSchema) {
		throw new Error('DATABRICKS_SCHEMA must be set to a per-run scratch schema name');
	}
});

describe('Integration harness', function() {
	this.timeout(60000);

	before(function() {
		if (!dbconfig) return this.skip();
		const connect = require('../../lib/connect.js');
		client = connect(dbconfig);
	});

	after(async function() {
		if (!client || !dbconfig) return;
		// Drop the per-run schema. Run manually if after() hook crashes:
		// DROP SCHEMA <catalog>.<schema> CASCADE
		const catalog = dbconfig.catalog;
		await new Promise((resolve, reject) => {
			client.query(
				`DROP SCHEMA IF EXISTS ${catalog}.${runSchema} CASCADE`,
				[],
				err => err ? reject(err) : resolve()
			);
		});
	});

	it('can create a scratch schema', async function() {
		if (!dbconfig) return this.skip();
		const catalog = dbconfig.catalog;
		await new Promise((resolve, reject) => {
			client.query(
				`CREATE SCHEMA IF NOT EXISTS ${catalog}.${runSchema}`,
				[],
				err => err ? reject(err) : resolve()
			);
		});
	});

	it('can query information_schema', async function() {
		if (!dbconfig) return this.skip();
		const catalog = dbconfig.catalog;
		await new Promise((resolve, reject) => {
			client.query(
				`SELECT schema_name FROM ${catalog}.information_schema.schemata WHERE schema_name = ?`,
				[runSchema],
				(err, rows) => {
					if (err) return reject(err);
					const { expect } = require('chai');
					expect(rows.map(r => r.schema_name)).to.include(runSchema);
					resolve();
				}
			);
		});
	});
});

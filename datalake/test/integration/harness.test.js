'use strict';

// Step 9 — Integration harness
// Verifies connectivity, schema accessibility, and UC RootLocation lookup.
// Skips all tests when required env vars are unset (exit 0 offline).
//
// Deferred: blocked on open questions #3, #5, #6 in BUILD_PLAN.md.
// Uses fixed schema names (public_stage_local for local dev, public_stage for CI);
// isolation is per-branch catalog, not per-run UUID schema.

const { expect } = require('chai');
const { getConfig, checkNonprod } = require('./helpers/databricks.js');

let dbconfig;
let client;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) return this.skip();
	checkNonprod(dbconfig.host);
});

describe('Integration harness', function() {
	this.timeout(60000);

	before(function() {
		if (!dbconfig) return this.skip();
		const connect = require('../../lib/connect.js');
		client = connect(dbconfig);
	});

	it('can execute a query', async function() {
		if (!dbconfig) return this.skip();
		await new Promise((resolve, reject) => {
			client.query('SELECT 1 AS n', [], (err, rows) => {
				if (err) return reject(err);
				expect(rows[0].n).to.equal(1);
				resolve();
			});
		});
	});

	it('target schema exists in information_schema', async function() {
		if (!dbconfig) return this.skip();
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

	it('RootLocation resolves to a valid S3 URI', async function() {
		if (!dbconfig) return this.skip();
		// Exercises _ensureStagingLocation — the first streamToTableFromS3 call will rely on this.
		await new Promise((resolve, reject) => {
			client.query(
				`DESCRIBE SCHEMA EXTENDED \`${dbconfig.catalog}\`.\`${dbconfig.schema}\``,
				[],
				(err, rows) => {
					if (err) return reject(err);
					const rootRow = (rows || []).find(r => r.database_description_item === 'RootLocation');
					expect(rootRow, 'RootLocation row missing — schema must have a managed storage location').to.exist;
					expect(rootRow.database_description_value).to.match(/^s3:\/\//);
					resolve();
				}
			);
		});
	});
});

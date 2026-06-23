'use strict';

// Step 11a — Idempotency test
// Loads the same 100 events twice with distinct audit dates. Verifies:
//   - row count stays at 100 (MERGE matches on natural key, doesn't duplicate)
//   - `_auditdate` updates to the second load's value

const { Readable } = require('stream');
const { expect } = require('chai');
const { getConfig, checkAllowedHost } = require('./helpers/databricks.js');
const { TABLE, tableDef, makeRecords } = require('./helpers/test_fact.js');
const dwconnectFactory = require('../../lib/dwconnect.js');

let dbconfig;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) return this.skip();
	checkAllowedHost(dbconfig.host);
});

describe('Idempotency: same batch twice', function() {
	this.timeout(300000);

	const qualifiedTable = () => `\`${dbconfig.catalog}\`.\`${dbconfig.schema}\`.\`${TABLE}\``;

	let client;
	let firstAuditdate;
	let secondAuditdate;

	before(async function() {
		if (!dbconfig) return this.skip();
		client = dwconnectFactory(dbconfig);
		// Clean slate: drop table so audit dates from prior round_trip don't contaminate.
		await runQuery(client, `DROP TABLE IF EXISTS ${qualifiedTable()}`);
		client.clearSchemaCache();
		await client.changeTableStructure({ [TABLE]: tableDef });
	});

	after(async function() {
		if (client) {
			try { await client.end(); } catch (e) { /* best-effort */ }
		}
	});

	it('first load sets row count to 100', async function() {
		if (!dbconfig) return this.skip();
		firstAuditdate = client.auditdate;
		await runImport(client, makeRecords(100));
		const rows = await runQuery(client, `SELECT COUNT(*) AS n FROM ${qualifiedTable()}`);
		expect(Number(rows[0].n)).to.equal(100);
	});

	it('second load of the same batch keeps row count at 100', async function() {
		if (!dbconfig) return this.skip();
		// Sleep briefly so the refreshed auditdate is strictly greater than the first.
		await new Promise(r => setTimeout(r, 1500));
		// Refresh the shared client's auditdate rather than creating a new factory.
		// The pool session is reused — only the audit timestamp changes.
		client.setAuditdate();
		secondAuditdate = client.auditdate;
		expect(secondAuditdate).to.not.equal(firstAuditdate);
		await runImport(client, makeRecords(100));
		const rows = await runQuery(client, `SELECT COUNT(*) AS n FROM ${qualifiedTable()}`);
		expect(Number(rows[0].n)).to.equal(100);
	});

	it('_auditdate updated to the second load', async function() {
		if (!dbconfig) return this.skip();
		// Compare against the second auditdate. auditdate is stored as a quoted literal
		// in dwConnect.auditdate (e.g., "'2026-05-26T19:01:23Z'"); strip the quotes for
		// the value comparison.
		// Databricks CAST(TIMESTAMP AS STRING) uses a space separator (`2026-05-26 18:30:29`),
		// while ISO uses a T. Normalize both sides to the same shape for comparison.
		const expected = secondAuditdate.replace(/'/g, '').replace(/Z$/, '').replace('T', ' ');
		const rows = await runQuery(
			client,
			`SELECT CAST(_auditdate AS STRING) AS ad FROM ${qualifiedTable()} WHERE id = 1`
		);
		expect(rows).to.have.length(1);
		expect(rows[0].ad).to.equal(expected);
	});
});

function runQuery(client, sql, params) {
	return new Promise((resolve, reject) => {
		client.query(sql, params || [], (err, rows) => err ? reject(err) : resolve(rows));
	});
}

function runImport(client, records) {
	return new Promise((resolve, reject) => {
		client.importFact(
			Readable.from(records, { objectMode: true }),
			TABLE,
			['id'],
			(err) => err ? reject(err) : resolve(),
			tableDef
		);
	});
}

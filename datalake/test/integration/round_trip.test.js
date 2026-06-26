'use strict';

// Step 10 — Integration: round-trip happy path
// Loads 100 synthetic events through the connector and verifies:
//   - row count = 100 unique by NK
//   - sampled rows match input values
//   - surrogate keys match Node-side fingerprint64 recomputation
//
// Throws (fails) when DATABRICKS_CONFIG_PROFILE + ~/.databrickscfg or DATABRICKS_HOST+auth env vars are not configured.

const { Readable } = require('stream');
const { expect } = require('chai');
const { getConfig, checkAllowedHost } = require('./helpers/databricks.js');
const { TABLE, tableDef, makeRecords } = require('./helpers/test_fact.js');
const fingerprint64 = require('../../lib/surrogate_key.js');
const dwconnectFactory = require('../../lib/dwconnect.js');

let dbconfig;
let client;

before(function() {
	dbconfig = getConfig();
	checkAllowedHost(dbconfig.host);
});

describe('Round-trip: 100-event fact load', function() {
	this.timeout(300000);

	const qualifiedTable = () => `\`${dbconfig.catalog}\`.\`${dbconfig.schema}\`.\`${TABLE}\``;

	before(async function() {
		client = dwconnectFactory(dbconfig);
		await runQuery(client, `DROP TABLE IF EXISTS ${qualifiedTable()}`);
		client.clearSchemaCache();
		const result = await client.changeTableStructure({ [TABLE]: tableDef });
		expect(result[TABLE]).to.equal('Added');
	});

	after(async function() {
		if (client) {
			try { await client.end(); } catch (e) { /* best-effort */ }
		}
	});

	it('loads 100 records via importFact', async function() {
		const records = makeRecords(100);
		await new Promise((resolve, reject) => {
			client.importFact(
				Readable.from(records, { objectMode: true }),
				TABLE,
				['id'],
				(err, tableInfo) => {
					if (err) return reject(err);
					try {
						expect(tableInfo, 'importFact must return a result object').to.exist;
						expect(tableInfo.count, 'count must equal staging row count').to.equal(100);
						resolve();
					} catch (e) {
						reject(e);
					}
				},
				tableDef
			);
		});

		const rows = await runQuery(client, `SELECT COUNT(*) AS n FROM ${qualifiedTable()}`);
		expect(Number(rows[0].n)).to.equal(100);
	});

	it('sampled rows match input', async function() {
		const expected = makeRecords(100);
		const sampleIds = [1, 50, 100];
		const rows = await runQuery(
			client,
			`SELECT id, name, status, is_active, score, quantity FROM ${qualifiedTable()} WHERE id IN (${sampleIds.join(',')}) ORDER BY id`
		);
		expect(rows).to.have.length(3);
		sampleIds.forEach((id, idx) => {
			const exp = expected[id - 1];
			const row = rows[idx];
			expect(Number(row.id)).to.equal(exp.id);
			expect(row.name).to.equal(exp.name);
			expect(row.status).to.equal(exp.status);
			expect(row.is_active).to.equal(exp.is_active);
			expect(Number(row.score)).to.equal(Number(exp.score));
			expect(Number(row.quantity)).to.equal(exp.quantity);
		});
	});

	it('surrogate keys match Node-side fingerprint64 recompute', async function() {
		// Cast BIGINT to STRING in SQL — JS Number loses precision past 2^53,
		// so reading `sk` as a Number would round and break the comparison.
		const rows = await runQuery(
			client,
			`SELECT id, CAST(sk AS STRING) AS sk FROM ${qualifiedTable()} WHERE id IN (1, 50, 100) ORDER BY id`
		);
		expect(rows).to.have.length(3);
		rows.forEach(row => {
			const expectedSk = fingerprint64([row.id]);
			expect(row.sk).to.equal(expectedSk);
		});
	});
});

function runQuery(client, sql, params) {
	return new Promise((resolve, reject) => {
		client.query(sql, params || [], (err, rows) => err ? reject(err) : resolve(rows));
	});
}

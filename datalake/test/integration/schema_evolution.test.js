'use strict';

// Step 11b — Schema evolution test
// Loads 100 events into a table; adds a new column to the dw_fields tableDef;
// runs changeTableStructure (emits ALTER TABLE ADD COLUMN); loads 10 more
// events with the new column populated. Verifies:
//   - the new column appears in DESCRIBE TABLE
//   - new rows have non-null values for the new column
//   - prior rows have null for the new column
//
// Requires the assumed service principal to have MODIFY on the target schema/table
// (BUILD_PLAN.md §5). If MODIFY is missing the ALTER will fail loudly.

const { Readable } = require('stream');
const { expect } = require('chai');
const { getConfig, checkAllowedHost } = require('./helpers/databricks.js');
const { TABLE, tableDef, makeRecords, tableDefWithExtraColumn } = require('./helpers/test_fact.js');
const dwconnectFactory = require('../../lib/dwconnect.js');

let dbconfig;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) return this.skip();
	checkAllowedHost(dbconfig.host);
});

describe('Schema evolution: add column mid-flight', function() {
	this.timeout(300000);

	const qualifiedTable = () => `\`${dbconfig.catalog}\`.\`${dbconfig.schema}\`.\`${TABLE}\``;

	let client;

	before(async function() {
		if (!dbconfig) return this.skip();
		client = dwconnectFactory(dbconfig);
		await runQuery(client, `DROP TABLE IF EXISTS ${qualifiedTable()}`);
		client.clearSchemaCache();
		const result = await client.changeTableStructure({ [TABLE]: tableDef });
		expect(result[TABLE]).to.equal('Added');
		await runImport(client, makeRecords(100));
	});

	after(async function() {
		if (client) {
			try { await client.end(); } catch (e) { /* best-effort */ }
		}
	});

	it('changeTableStructure with extra_col reports Modified and adds the column', async function() {
		if (!dbconfig) return this.skip();
		client.clearSchemaCache();
		const result = await client.changeTableStructure({ [TABLE]: tableDefWithExtraColumn() });
		expect(result[TABLE]).to.equal('Modified');

		const cols = await runQuery(
			client,
			`SELECT column_name, data_type FROM ${dbconfig.catalog}.information_schema.columns
			 WHERE table_schema = ? AND table_name = ? AND column_name = 'extra_col'`,
			[dbconfig.schema, TABLE]
		);
		expect(cols).to.have.length(1);
		expect(cols[0].data_type).to.equal('STRING');
	});

	it('loads 10 more rows with extra_col set; prior rows show null', async function() {
		if (!dbconfig) return this.skip();
		const extraRecords = [];
		for (let i = 101; i <= 110; i++) {
			extraRecords.push(Object.assign({}, makeRecords(i)[i - 1], {
				extra_col: `new_value_${i}`,
			}));
		}
		await runImport(client, extraRecords, tableDefWithExtraColumn());

		const total = await runQuery(client, `SELECT COUNT(*) AS n FROM ${qualifiedTable()}`);
		expect(Number(total[0].n)).to.equal(110);

		const newRows = await runQuery(client, `SELECT id, extra_col FROM ${qualifiedTable()} WHERE id BETWEEN 101 AND 110 ORDER BY id`);
		expect(newRows).to.have.length(10);
		newRows.forEach(r => expect(r.extra_col).to.equal(`new_value_${Number(r.id)}`));

		const oldRows = await runQuery(client, `SELECT id, extra_col FROM ${qualifiedTable()} WHERE id BETWEEN 1 AND 5 ORDER BY id`);
		expect(oldRows).to.have.length(5);
		oldRows.forEach(r => expect(r.extra_col, `id ${r.id} should have null extra_col`).to.be.null);
	});
});

function runQuery(client, sql, params) {
	return new Promise((resolve, reject) => {
		client.query(sql, params || [], (err, rows) => err ? reject(err) : resolve(rows));
	});
}

function runImport(client, records, def) {
	return new Promise((resolve, reject) => {
		client.importFact(
			Readable.from(records, { objectMode: true }),
			TABLE,
			['id'],
			(err) => err ? reject(err) : resolve(),
			def || tableDef
		);
	});
}

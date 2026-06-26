'use strict';

// Integration: soft-delete path for facts with string natural keys.
//
// Loads a small set of records whose SKU values contain characters that are
// historically problematic for inline SQL literals: embedded single-quotes,
// trailing backslashes, and backslash-before-quote sequences. Then deletes
// those rows via __leo_delete__ markers and verifies _deleted = true only for
// the targeted rows.

const { Readable } = require('stream');
const { expect } = require('chai');
const { getConfig, checkAllowedHost } = require('./helpers/databricks.js');
const dwconnectFactory = require('../../lib/dwconnect.js');

const TABLE = 'f_datalake_connector_delete_test';

const tableDef = {
	structure: {
		sku: 'varchar(100)',
		sk: 'sk',
		name: 'varchar(255)',
	},
	nk: ['sku'],
	isDimension: false,
};

const TRICKY_SKUS = [
	"it's",           // embedded single quote
	'foo\\',          // trailing backslash
	"foo\\'bar",      // backslash immediately before quote
];
const SAFE_SKU = 'normal-sku-001';

const ALL_SKUS = [...TRICKY_SKUS, SAFE_SKU];

let dbconfig;
let client;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) return this.skip();
	checkAllowedHost(dbconfig.host);
});

describe('Soft-delete: tricky string natural keys', function() {
	this.timeout(300000);

	const qualifiedTable = () => `\`${dbconfig.catalog}\`.\`${dbconfig.schema}\`.\`${TABLE}\``;

	before(async function() {
		if (!dbconfig) return this.skip();
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

	it('loads initial records including tricky-character SKUs', async function() {
		if (!dbconfig) return this.skip();
		const records = ALL_SKUS.map(sku => ({ sku, name: `name for ${sku}` }));
		await importFact(client, TABLE, ['sku'], tableDef, records);
		const rows = await runQuery(client, `SELECT COUNT(*) AS n FROM ${qualifiedTable()}`);
		expect(Number(rows[0].n)).to.equal(ALL_SKUS.length);
	});

	it('soft-deletes tricky-character SKUs via __leo_delete__ markers', async function() {
		if (!dbconfig) return this.skip();
		const deletes = TRICKY_SKUS.map(sku => ({
			__leo_delete__: 'sku',
			__leo_delete_id__: sku,
		}));
		await importFact(client, TABLE, ['sku'], tableDef, deletes);

		const rows = await runQuery(
			client,
			`SELECT sku, _deleted FROM ${qualifiedTable()} ORDER BY sku`
		);
		const bySkU = {};
		rows.forEach(r => { bySkU[r.sku] = r._deleted; });

		TRICKY_SKUS.forEach(sku => {
			expect(bySkU[sku], `expected _deleted=true for sku: ${sku}`).to.equal(true);
		});
		expect(bySkU[SAFE_SKU], 'safe SKU must not be deleted').to.equal(false);
	});
});

function importFact(client, table, ids, tableDef, records) {
	return new Promise((resolve, reject) => {
		client.importFact(
			Readable.from(records, { objectMode: true }),
			table,
			ids,
			(err, result) => err ? reject(err) : resolve(result),
			tableDef
		);
	});
}

function runQuery(client, sql) {
	return new Promise((resolve, reject) => {
		client.query(sql, [], (err, rows) => err ? reject(err) : resolve(rows));
	});
}

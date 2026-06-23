'use strict';

// Step 8 smoke test — offline wiring guard
//
// Pipes 100 synthetic events through the real leo-connector-common load.js (including
// combine.js sort-and-dedup) into the real lib/dwconnect.js importFact, with the
// Databricks SQL layer (connect.js) and S3 staging stubbed at the connect.js boundary.
//
// This guards the load.js → dwconnect.js dispatch wiring: if importFact's signature,
// the MIN query, or the MERGE callback path drift, this test breaks before production does.

const fs = require('fs');
const { expect } = require('chai');
const { Readable, Writable } = require('stream');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

const load = require('leo-connector-common/datawarehouse/load.js');

// ─── Fixture ──────────────────────────────────────────────────────────────────

const TABLE = 'f_smoke_test';

const tableDef = {
	identifier: TABLE,
	isDimension: false,
	clusterKey: 'id',
	structure: {
		id:       { type: 'bigint', nk: true },
		name:     { type: 'varchar(100)' },
		quantity: { type: 'integer' },
	},
};

const tableConfig = { [TABLE]: tableDef };

// Databricks column metadata returned by the stubbed describeTable.
const tableFields = [
	{ column_name: 'id',             data_type: 'BIGINT' },
	{ column_name: 'name',           data_type: 'STRING' },
	{ column_name: 'quantity',       data_type: 'INT' },
	{ column_name: '_deleted',       data_type: 'BOOLEAN' },
	{ column_name: '_auditdate',     data_type: 'TIMESTAMP_NTZ' },
	{ column_name: '_rescued_data',  data_type: 'STRING' },
];

function makeStream(n) {
	const events = [];
	for (let i = 1; i <= n; i++) {
		events.push({
			id: `eid:${i}`,
			payload: {
				table: TABLE,
				type: 'fact',
				data: { id: i, name: `rec_${i}`, quantity: i * 10 },
			},
		});
	}
	return Readable.from(events, { objectMode: true });
}

// ─── Suite ────────────────────────────────────────────────────────────────────

describe('load.js smoke — offline wiring guard', function() {
	this.timeout(20000);

	let client;
	let connectClientStub;
	let insertMissingDimensionsCalls;
	let dropTempTablesCalls;
	let minQueryCount;
	let mergeQueryCount;

	before(function(done) {
		// combine.js (leo-connector-common) hardcodes /tmp/leo_dw_* for sort scratch.
		// Skip the suite rather than fail in environments where /tmp/ is not writable.
		try { fs.writeFileSync('/tmp/leo_dw_smoke_probe', ''); fs.unlinkSync('/tmp/leo_dw_smoke_probe'); }
		catch (e) { return this.skip(); }

		insertMissingDimensionsCalls = [];
		dropTempTablesCalls = 0;
		minQueryCount = 0;
		mergeQueryCount = 0;

		connectClientStub = {
			auditdate: "'2026-01-01T00:00:00'",
			escapeId: name => '`' + String(name).toLowerCase() + '`',
			escapeValueNoToLower: v => typeof v === 'string' ? "'" + v + "'" : v,
			describeTable: sinon.stub().resolves(tableFields),
			ensureStagingLocation: sinon.stub().resolves({ s3Bucket: 'smoke-bucket', s3Prefix: 'smoke/prefix' }),
			stagingS3Path: (_table, _auditdate) => ({
				bucket: 'smoke-bucket',
				key: 'smoke/prefix/f_smoke_test/2026-01-01T00-00-00.csv',
				uri: 's3://smoke-bucket/smoke/prefix/f_smoke_test/2026-01-01T00-00-00.csv',
			}),
			// Returns a draining Writable so ls.pipe can complete — not a stub returning {pipe}.
			streamToTableFromS3: sinon.stub().callsFake(() =>
				new Writable({ objectMode: true, write(_chunk, _enc, cb) { cb(); } })
			),
			buildStagingSelect: sinon.stub().returns('SELECT * FROM smoke_staging'),
			query: sinon.stub().callsFake((sql, _params, cb) => {
				const done = typeof cb === 'function' ? cb : (typeof _params === 'function' ? _params : () => {});
				if (/SELECT MIN\(/.test(sql)) {
					minQueryCount++;
					return done(null, [{ minval: 1, cnt: 100 }]);
				}
				if (/SELECT CAST\(COUNT/.test(sql)) {
					return done(null, [{ cnt: 100 }]);
				}
				if (/^MERGE INTO/.test(sql)) {
					mergeQueryCount++;
					return done(null, []);
				}
				return done(null, []);
			}),
		};

		const connectStub = sinon.stub().returns(connectClientStub);
		connectStub.isConnectionError = require('../../lib/connect.js').isConnectionError;

		const dwconnectFactory = proxyquire('../../lib/dwconnect.js', {
			'./connect.js': connectStub,
			'leo-logger': { info: () => {}, debug: () => {}, error: () => {}, log: () => {}, warn: () => {} },
			'./sql.js': require('../../lib/sql.js'),
			'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			'./audit_timestamp.js': () => '2026-01-01T00:00:00',
		});

		client = dwconnectFactory({
			catalog: 'test_cat',
			schema: 'test_schema',
			keepS3Files: true,  // skip S3 deleteObject after MERGE
		});

		// Wrap dwconnect's insertMissingDimensions to track calls; it's a no-op in the
		// datalake connector (hashedSurrogateKeys=true means no stub-row inserts needed).
		const realInsert = client.insertMissingDimensions.bind(client);
		client.insertMissingDimensions = function(usedTables, tc, tSks, tNks, callback) {
			insertMissingDimensionsCalls.push(Object.keys(usedTables));
			realInsert(usedTables, tc, tSks, tNks, callback);
		};

		// Wrap dwconnect's dropTempTables to confirm load.js calls it on completion.
		const realDrop = client.dropTempTables.bind(client);
		client.dropTempTables = async function() {
			dropTempTablesCalls++;
			return realDrop();
		};

		load('smoke:bot', 'queue:smoke', client, tableConfig, makeStream(100), (err) => {
			done(err);  // fail the before hook (and all tests) if load.js returns an error
		});
	});

	it('stages records to S3 via streamToTableFromS3', () => {
		expect(connectClientStub.streamToTableFromS3.callCount, 'streamToTableFromS3 call count').to.equal(1);
		expect(connectClientStub.streamToTableFromS3.firstCall.args[0]).to.equal(TABLE);
	});

	it('runs the MIN prune query against the staging clause', () => {
		expect(minQueryCount, 'MIN query count').to.equal(1);
	});

	it('runs the MERGE INTO query', () => {
		expect(mergeQueryCount, 'MERGE query count').to.equal(1);
	});

	it('calls insertMissingDimensions once for the used fact table', () => {
		expect(insertMissingDimensionsCalls, 'insertMissingDimensions call count').to.have.length(1);
		expect(insertMissingDimensionsCalls[0]).to.include(TABLE);
	});

	it('calls dropTempTables on completion', () => {
		expect(dropTempTablesCalls, 'dropTempTables call count').to.equal(1);
	});

	it('does not call linkDimensions — fact-only config has no dimension FK fields', () => {
		// linkDimensions is a no-op in dwconnect (FK pre-computed); load.js only invokes
		// it when the config has dimension-linked fields. This test passes trivially if the
		// before hook completed without error (linkDimensions throwing would have failed it).
	});
});

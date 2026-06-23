'use strict';

const { expect } = require('chai');
const { PassThrough } = require('stream');
const csv = require('fast-csv');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// Test nonNull/CSV serialization directly — this is the function from connect.js
// duplicated here for unit testing the serialization contract.
function nonNull(v) {
	if (v === '' || v === null || v === undefined) return '\\N';
	if (typeof v === 'string' && v.search(/\r/) !== -1) return v.replace(/\r\n?/g, '\n');
	return v;
}

describe('importFact — nonNull / CSV serialization contract', () => {
	it('serializes null as \\N', () => {
		expect(nonNull(null)).to.equal('\\N');
	});

	it('serializes undefined as \\N', () => {
		expect(nonNull(undefined)).to.equal('\\N');
	});

	it('serializes empty string as \\N', () => {
		expect(nonNull('')).to.equal('\\N');
	});

	it('normalizes \\r\\n to \\n', () => {
		expect(nonNull('foo\r\nbar')).to.equal('foo\nbar');
	});

	it('normalizes \\r to \\n', () => {
		expect(nonNull('foo\rbar')).to.equal('foo\nbar');
	});

	it('passes through bare \\n unchanged', () => {
		expect(nonNull('foo\nbar')).to.equal('foo\nbar');
	});

	it('passes through normal string unchanged', () => {
		expect(nonNull('hello world')).to.equal('hello world');
	});

	it('passes through numbers unchanged', () => {
		expect(nonNull(42)).to.equal(42);
	});

	it('serializes booleans as-is (true/false strings handled by fast-csv)', () => {
		expect(nonNull(true)).to.equal(true);
		expect(nonNull(false)).to.equal(false);
	});
});

describe('importFact — CSV output contract', () => {
	function csvRowsFromObjects(objects, columns) {
		return new Promise((resolve, reject) => {
			const ws = csv.createWriteStream({
				headers: false,
				delimiter: '|',
				transform: (row, done) => done(null, columns.map(f => nonNull(row[f]))),
			});
			const output = new PassThrough();
			let buf = '';
			output.on('data', d => { buf += d.toString(); });
			output.on('end', () => resolve(buf));
			output.on('error', reject);
			ws.pipe(output);
			objects.forEach(o => ws.write(o));
			ws.end(() => {});
		});
	}

	it('pipe-delimits fields', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: 'foo', active: true }],
			['id', 'name', 'active']
		);
		expect(output.trim()).to.equal('1|foo|true');
	});

	it('serializes null as \\N in CSV output', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: null }],
			['id', 'name']
		);
		expect(output.trim()).to.equal('1|\\N');
	});

	it('serializes empty string as \\N', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: '' }],
			['id', 'name']
		);
		expect(output.trim()).to.equal('1|\\N');
	});

	it('quotes a field containing a pipe character', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: 'foo|bar' }],
			['id', 'name']
		);
		// fast-csv RFC-4180 quoting wraps the field in double-quotes
		expect(output.trim()).to.equal('1|"foo|bar"');
	});

	it('collapses \\r\\n to \\n in a string field', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, notes: 'line1\r\nline2' }],
			['id', 'notes']
		);
		expect(output).to.include('line1\nline2');
		expect(output).to.not.include('\r');
	});

	it('serializes boolean true as "true" string', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, active: true }],
			['id', 'active']
		);
		expect(output.trim()).to.equal('1|true');
	});

	it('serializes boolean false as "false" string', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, active: false }],
			['id', 'active']
		);
		expect(output.trim()).to.equal('1|false');
	});
});

// ─────────────────────────────────────────────────────────────────────────
// Orchestration tests: cover importFact's wiring of describeTable →
// ensureStagingLocation → streamToTableFromS3 → MERGE, plus prune-query
// type-aware quoting, error propagation, and the row-level routing
// (delete records out, sk + audit + _deleted onto every survivor).
//
// leo-sdk.streams is the canonical leo-streams package (cf. connect.js);
// we stub it so ls.pipe completes synchronously and ls.through hands us
// the per-row callbacks for direct invocation.
// ─────────────────────────────────────────────────────────────────────────

describe('importFact — orchestration', () => {
	function setup(opts) {
		opts = opts || {};
		const queryHistory = [];
		const throughCallbacks = [];
		let pipeFinalCb = null;

		const clientStub = {
			auditdate: "'2026-05-27T00:00:00'",
			setAuditdate: sinon.stub(),
			escapeId: name => '`' + String(name).toLowerCase() + '`',
			escapeValueNoToLower: v => typeof v === 'string' ? "'" + v.replace(/'/g, "\\'") + "'" : v,
			describeTable: sinon.stub().resolves(opts.tableFields),
			ensureStagingLocation: sinon.stub().resolves({ s3Bucket: 'b', s3Prefix: 'p' }),
			// importFact now owns the staging-path identifier and passes it down
			// to streamToTableFromS3; the stub returns a deterministic per-call
			// {bucket, key, uri} so the assertions can find the MERGE without
			// reading any back-channel state from the client.
			stagingS3Path: (table) => ({
				bucket: 'bucket',
				key: `p/${table}/2026-05-27T00-00-00.csv`,
				uri: `s3://bucket/p/${table}/2026-05-27T00-00-00.csv`,
			}),
			streamToTableFromS3: sinon.stub().returns({ pipe: sinon.stub() }),
			buildStagingSelect: sinon.stub().returns('SELECT * FROM read_files(...)'),
			query: sinon.stub().callsFake((sql, params, cb) => {
				queryHistory.push(sql);
				const finalCb = typeof cb === 'function' ? cb : (typeof params === 'function' ? params : () => {});
				if (opts.failFlushDeletes && /^UPDATE\s/i.test(sql)) {
					return finalCb(new Error('flush delete failed'));
				}
				if (sql.indexOf('SELECT MIN(') === 0) {
					if (opts.failMinQuery) return finalCb(new Error('MIN query failed'));
					return finalCb(null, [{ minval: opts.minVal !== undefined ? opts.minVal : 100, cnt: opts.cnt !== undefined ? opts.cnt : 50 }]);
				}
				if (sql.indexOf('SELECT CAST(COUNT(') === 0) {
					if (opts.failCountQuery) return finalCb(new Error('COUNT query failed'));
					return finalCb(null, [{ cnt: opts.cnt !== undefined ? opts.cnt : 50 }]);
				}
				return finalCb(null, []);
			}),
		};

		const lsStub = {
			through: cb => { throughCallbacks.push(cb); return { pipe: sinon.stub() }; },
			pipe: (...args) => { pipeFinalCb = args[args.length - 1]; },
		};

		// connect.js exports isConnectionError as a named property; dwconnect.js
		// destructures it at require-time, so the stub must carry it too.
		const connectStub = sinon.stub().returns(clientStub);
		connectStub.isConnectionError = require('../../lib/connect.js').isConnectionError;

		const factory = proxyquire('../../lib/dwconnect.js', {
			'./connect.js': connectStub,
			'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
			'leo-sdk': {
				streams: lsStub,
				aws: { s3: { deleteObject: (p, cb) => cb(null) } },
			},
			'./sql.js': require('../../lib/sql.js'),
			'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			// Pin the audit timestamp so enrichment assertions are deterministic.
			// The factory's setAuditdate() would otherwise stamp client.auditdate
			// with naiveIsoNow() at construction time and clobber our stub value.
			'./audit_timestamp.js': () => '2026-05-27T00:00:00',
		});

		const dwClient = factory(opts.dbconfig || { catalog: 'cat', schema: 'sch' });

		return {
			dwClient,
			clientStub,
			queryHistory,
			throughCallbacks,
			completePipeline: (err) => { pipeFinalCb(err || null); },
		};
	}

	function callImportFact(dwClient, table, ids, tableDef) {
		return new Promise((resolve, reject) => {
			dwClient.importFact(null, table, ids, (err, result) => err ? reject(err) : resolve(result), tableDef);
		});
	}

	// Drive importFact through to completion. The promise resolves when the
	// final callback fires (after MERGE + cleanupStagedFile). pipeline is
	// completed synchronously after a tick so the describeTable + ensureStagingLocation
	// promises have settled.
	async function runToCompletion(ctx, table, ids, tableDef, pipelineError) {
		const p = callImportFact(ctx.dwClient, table, ids, tableDef);
		// Allow the describeTable/ensureStagingLocation promise chain to register ls.pipe.
		await new Promise(setImmediate);
		ctx.completePipeline(pipelineError);
		return p;
	}

	const factTableFields = [
		{ column_name: 'id', data_type: 'BIGINT' },
		{ column_name: 'qty', data_type: 'INT' },
		{ column_name: 'created_at', data_type: 'TIMESTAMP_NTZ' },
		{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
		{ column_name: '_deleted', data_type: 'BOOLEAN' },
	];

	it('emits MERGE INTO against the qualified target table', async () => {
		const ctx = setup({ tableFields: factTableFields });
		await runToCompletion(ctx, 'f_order_item', ['id']);
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge, 'expected a MERGE INTO query').to.exist;
		expect(merge).to.include('cat.sch.f_order_item');
	});

	it('runs a SELECT MIN prune query against the single NK', async () => {
		const ctx = setup({ tableFields: factTableFields });
		await runToCompletion(ctx, 'f_order_item', ['id']);
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery, 'expected a prune MIN query').to.exist;
		expect(minQuery).to.include('`id`');
	});

	it('prefers tableDef.clusterKey over ids as the prune column', async () => {
		const ctx = setup({ tableFields: factTableFields });
		await runToCompletion(ctx, 'f_order_item', ['id'], { clusterKey: 'qty' });
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery).to.include('`qty`');
		expect(minQuery).to.not.include('MIN(`id`)');
	});

	it('prune filter is NOT injected into the MERGE ON clause (regression: would duplicate rows with old cluster keys)', async () => {
		const ctx = setup({ tableFields: factTableFields, minVal: 42 });
		await runToCompletion(ctx, 'f_order_item', ['id']); // no clusterKey — pruneCol falls back to single NK
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge, 'expected a MERGE INTO query').to.exist;
		expect(merge).to.not.include('>=');
	});

	it('naturalKeyFilter is computed but NOT injected into MERGE ON (numeric case)', async () => {
		const ctx = setup({ tableFields: factTableFields, minVal: 12345 });
		await runToCompletion(ctx, 'f_order_item', ['id'], { clusterKey: 'id' });
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge).to.not.include('>=');
	});

	it('naturalKeyFilter is computed but NOT injected into MERGE ON (timestamp case)', async () => {
		const ctx = setup({ tableFields: factTableFields, minVal: '2026-03-15 14:30:00' });
		await runToCompletion(ctx, 'f_order_item', ['id'], { clusterKey: 'created_at' });
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge).to.not.include('>=');
	});

	it('skips prune query for composite NKs when no clusterKey is set', async () => {
		const ctx = setup({ tableFields: factTableFields });
		await runToCompletion(ctx, 'f_order_item', ['id', 'qty']);
		expect(ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0)).to.not.exist;
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.exist;
	});

	it('normalizes a single non-array id to an array', async () => {
		const ctx = setup({ tableFields: factTableFields });
		await runToCompletion(ctx, 'f_order_item', 'id');
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery).to.include('`id`');
	});

	it('propagates a pipeline error to the callback and skips MERGE', async () => {
		const ctx = setup({ tableFields: factTableFields });
		let caught;
		try {
			await runToCompletion(ctx, 'f_order_item', ['id'], undefined, new Error('pipe failed'));
		} catch (err) {
			caught = err;
		}
		expect(caught).to.exist;
		expect(caught.message).to.equal('pipe failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('dataStream callback filters __leo_delete__ markers from the staging path', async () => {
		const ctx = setup({ tableFields: factTableFields });
		const runP = runToCompletion(ctx, 'f_order_item', ['id']);
		await runP;
		// throughCallbacks[0] is the dataStream filter (registered first)
		const dataStreamCb = ctx.throughCallbacks[0];
		const forwarded = [];
		dataStreamCb({ __leo_delete__: 'id', __leo_delete_id__: 42 }, (err, obj) => {
			if (obj !== undefined) forwarded.push(obj);
		});
		dataStreamCb({ id: 1, qty: 5 }, (err, obj) => {
			if (obj !== undefined) forwarded.push(obj);
		});
		expect(forwarded).to.deep.equal([{ id: 1, qty: 5 }]);
	});

	it('returns staging cnt as result.count (single NK — pruneCol path)', async () => {
		const ctx = setup({ tableFields: factTableFields, cnt: 77 });
		const result = await runToCompletion(ctx, 'f_order_item', ['id']);
		expect(result).to.exist;
		expect(result.count).to.equal(77);
	});

	it('returns staging cnt as result.count (composite NK — COUNT path)', async () => {
		const ctx = setup({ tableFields: factTableFields, cnt: 33 });
		const result = await runToCompletion(ctx, 'f_order_item', ['id', 'qty']);
		expect(result).to.exist;
		expect(result.count).to.equal(33);
	});

	it('propagates a flushDeletes error and skips MERGE', async () => {
		const ctx = setup({ tableFields: factTableFields, failFlushDeletes: true });
		const p = callImportFact(ctx.dwClient, 'f_order_item', ['id']);
		// dataStream cb (throughCallbacks[0]) is registered synchronously in importFact;
		// inject a delete record so flushDeletes runs a real UPDATE and hits the error.
		ctx.throughCallbacks[0]({ __leo_delete__: 'id', __leo_delete_id__: 42 }, () => {});
		await new Promise(setImmediate);
		ctx.completePipeline();
		let caught;
		try { await p; } catch (e) { caught = e; }
		expect(caught, 'expected an error from flushDeletes').to.exist;
		expect(caught.message).to.equal('flush delete failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('skips flushDeletes UPDATE when __leo_delete__ names a non-existent column', async () => {
		const ctx = setup({ tableFields: factTableFields });
		const p = callImportFact(ctx.dwClient, 'f_order_item', ['id']);
		ctx.throughCallbacks[0]({ __leo_delete__: 'bogus_column', __leo_delete_id__: 99 }, () => {});
		await new Promise(setImmediate);
		ctx.completePipeline();
		await p;
		expect(ctx.queryHistory.find(q => q.startsWith('UPDATE'))).to.not.exist;
	});

	it('propagates a MIN query error and skips MERGE', async () => {
		const ctx = setup({ tableFields: factTableFields, failMinQuery: true });
		let caught;
		try {
			await runToCompletion(ctx, 'f_order_item', ['id']);
		} catch (e) {
			caught = e;
		}
		expect(caught, 'expected an error from MIN query').to.exist;
		expect(caught.message).to.equal('MIN query failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('propagates a COUNT query error and skips MERGE (composite NK path)', async () => {
		const ctx = setup({ tableFields: factTableFields, failCountQuery: true });
		let caught;
		try {
			await runToCompletion(ctx, 'f_order_item', ['id', 'qty']);
		} catch (e) {
			caught = e;
		}
		expect(caught, 'expected an error from COUNT query').to.exist;
		expect(caught.message).to.equal('COUNT query failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('enrichedStream stamps audit/_deleted and computes sk via fingerprint64', async () => {
		const ctx = setup({
			tableFields: [
				...factTableFields,
				{ column_name: '_id', data_type: 'BIGINT' },
			],
		});
		const tableDef = {
			structure: {
				'_id': 'sk',
				'id': { nk: true, type: 'integer' },
				'qty': { type: 'integer' },
			},
		};
		await runToCompletion(ctx, 'f_order_item', ['id'], tableDef);
		// throughCallbacks[1] is the enrichment stream (registered after the filter)
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ id: 42, qty: 5 }, (err, obj) => { enriched = obj; });
		expect(enriched.id).to.equal(42);
		expect(enriched.qty).to.equal(5);
		expect(enriched._auditdate).to.equal('2026-05-27T00:00:00');
		expect(enriched._deleted).to.equal(false);
		// _id is the sk column → fingerprint64 of natural-key values
		expect(typeof enriched._id).to.equal('string');
		expect(enriched._id).to.match(/^-?\d+$/);
	});

	it('enrichedStream computes entity FK hash for dimension field', async () => {
		const ctx = setup({
			tableFields: [
				{ column_name: 'id', data_type: 'BIGINT' },
				{ column_name: 'item_id', data_type: 'INT' },
				{ column_name: 'd_item', data_type: 'BIGINT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
			],
		});
		const tableDef = {
			structure: {
				'id': { nk: true, type: 'integer' },
				'item_id': { type: 'integer', dimension: 'd_item' },
			},
		};
		await runToCompletion(ctx, 'f_order_item', ['id'], tableDef);
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ id: 1, item_id: 99 }, (err, obj) => { enriched = obj; });
		// d_item = fingerprint64(['99']) — a signed 64-bit decimal string
		expect(typeof enriched.d_item).to.equal('string');
		expect(enriched.d_item).to.match(/^-?\d+$/);
		// Must match fingerprint64 of the same value
		const fingerprint64 = require('../../lib/surrogate_key.js');
		expect(enriched.d_item).to.equal(fingerprint64([99]));
	});

	it('enrichedStream writes "1" for null entity FK', async () => {
		const ctx = setup({
			tableFields: [
				{ column_name: 'id', data_type: 'BIGINT' },
				{ column_name: 'item_id', data_type: 'INT' },
				{ column_name: 'd_item', data_type: 'BIGINT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
			],
		});
		const tableDef = {
			structure: {
				'id': { nk: true, type: 'integer' },
				'item_id': { type: 'integer', dimension: 'd_item' },
			},
		};
		await runToCompletion(ctx, 'f_order_item', ['id'], tableDef);
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ id: 1, item_id: null }, (err, obj) => { enriched = obj; });
		expect(enriched.d_item).to.equal('1');
	});

	it('enrichedStream computes _date and _time for datetime dimension field', async () => {
		const ctx = setup({
			tableFields: [
				{ column_name: 'id', data_type: 'BIGINT' },
				{ column_name: 'occurred_at', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: 'd_occurred_at_date', data_type: 'INT' },
				{ column_name: 'd_occurred_at_time', data_type: 'INT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
			],
		});
		const tableDef = {
			structure: {
				'id': { nk: true, type: 'integer' },
				'occurred_at': { type: 'timestamp', dimension: 'datetime' },
			},
		};
		await runToCompletion(ctx, 'f_order_item', ['id'], tableDef);
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ id: 1, occurred_at: '1400-01-01T14:30:45' }, (err, obj) => { enriched = obj; });
		expect(enriched.d_occurred_at_date).to.equal(10000); // 1400-01-01 → offset 0 + 10000
		expect(enriched.d_occurred_at_time).to.equal(62245); // 14*3600+30*60+45+10000
	});
});

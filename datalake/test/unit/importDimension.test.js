'use strict';

const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// ─────────────────────────────────────────────────────────────────────────────
// Orchestration tests for importDimension: covers the S3-staging + MERGE wiring,
// prune-query type-aware quoting, error propagation, delete-marker filtering,
// and row enrichment (sk + auditdate, no _deleted).
// ─────────────────────────────────────────────────────────────────────────────

describe('importDimension — orchestration', () => {
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
				if (sql.indexOf('SELECT MIN(') === 0) {
					if (opts.failMinQuery) return finalCb(new Error('MIN query failed'));
					return finalCb(null, [{ minval: opts.minVal !== undefined ? opts.minVal : 100, cnt: opts.cnt !== undefined ? opts.cnt : 50 }]);
				}
				if (sql.indexOf('SELECT CAST(COUNT(') === 0) {
					if (opts.failCountQuery) return finalCb(new Error('COUNT query failed'));
					return finalCb(null, [{ cnt: opts.cnt !== undefined ? opts.cnt : 50 }]);
				}
				if (sql.startsWith('UPDATE') && opts.failDimDeleteUpdate) return finalCb(new Error('UPDATE failed'));
				return finalCb(null, []);
			}),
		};

		const lsStub = {
			through: cb => { throughCallbacks.push(cb); return { pipe: sinon.stub() }; },
			pipe: (...args) => { pipeFinalCb = args[args.length - 1]; },
		};

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

	function callImportDimension(dwClient, table, nk, tableDef) {
		return new Promise((resolve, reject) => {
			dwClient.importDimension(null, table, null, nk, {}, (err, result) => err ? reject(err) : resolve(result), tableDef);
		});
	}

	async function runToCompletion(ctx, table, nk, tableDef, pipelineError) {
		const p = callImportDimension(ctx.dwClient, table, nk, tableDef);
		await new Promise(setImmediate);
		ctx.completePipeline(pipelineError);
		return p;
	}

	const dimTableFields = [
		{ column_name: 'retailer_id', data_type: 'BIGINT' },
		{ column_name: 'name', data_type: 'STRING' },
		{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
		{ column_name: '_startdate', data_type: 'TIMESTAMP_NTZ' },
		{ column_name: '_enddate', data_type: 'TIMESTAMP_NTZ' },
		{ column_name: '_current', data_type: 'BOOLEAN' },
	];

	it('emits MERGE INTO against the qualified target table', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id']);
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge, 'expected a MERGE INTO query').to.exist;
		expect(merge).to.include('cat.sch.d_account');
	});

	it('emits a dim MERGE with sentinel values for new rows', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id']);
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge).to.include("'1900-01-01 00:00:00'");
		expect(merge).to.include("'9999-01-01 00:00:00'");
		expect(merge).to.include('true');
	});

	it('does NOT include _deleted in the MERGE SQL', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id']);
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge).to.not.include('_deleted');
	});

	it('runs a SELECT MIN prune query against the single NK', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id']);
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery, 'expected a prune MIN query').to.exist;
		expect(minQuery).to.include('`retailer_id`');
	});

	it('prune filter is NOT injected into the MERGE ON clause (regression: would duplicate rows with old cluster keys)', async () => {
		const ctx = setup({ tableFields: dimTableFields, minVal: 99 });
		await runToCompletion(ctx, 'd_account', ['retailer_id']); // no clusterKey — pruneCol falls back to single NK
		const merge = ctx.queryHistory.find(q => q.startsWith('MERGE INTO'));
		expect(merge, 'expected a MERGE INTO query').to.exist;
		expect(merge).to.not.include('>=');
	});

	it('prefers tableDef.clusterKey over nk as the prune column', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id'], { clusterKey: 'name' });
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery).to.include('`name`');
		expect(minQuery).to.not.include('MIN(`retailer_id`)');
	});

	it('skips prune query for composite NKs when no clusterKey is set', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id', 'name']);
		expect(ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0)).to.not.exist;
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.exist;
	});

	it('normalizes a single non-array nk to an array', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', 'retailer_id');
		const minQuery = ctx.queryHistory.find(q => q.indexOf('SELECT MIN(') === 0);
		expect(minQuery).to.include('`retailer_id`');
	});

	it('propagates a pipeline error to the callback and skips MERGE', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		let caught;
		try {
			await runToCompletion(ctx, 'd_account', ['retailer_id'], undefined, new Error('pipe failed'));
		} catch (err) {
			caught = err;
		}
		expect(caught).to.exist;
		expect(caught.message).to.equal('pipe failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('dataStream callback collects non-id __leo_delete__ markers without staging them', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		const runP = runToCompletion(ctx, 'd_account', ['retailer_id']);
		await runP;
		// throughCallbacks[0] is the dataStream delete filter
		const dataStreamCb = ctx.throughCallbacks[0];
		const forwarded = [];
		const noop = () => {};
		// Non-id delete: should NOT be forwarded to staging (push not called)
		dataStreamCb({ __leo_delete__: 'retailer_id', __leo_delete_id__: 42 }, noop, obj => forwarded.push(obj));
		// Normal object: should flow through via done(null, obj)
		dataStreamCb({ retailer_id: 1, name: 'Acme' }, (err, obj) => {
			if (obj !== undefined) forwarded.push(obj);
		}, noop);
		expect(forwarded).to.deep.equal([{ retailer_id: 1, name: 'Acme' }]);
	});

	it('dataStream callback pushes id-marked deletes to staging', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		await runToCompletion(ctx, 'd_account', ['retailer_id']);
		const dataStreamCb = ctx.throughCallbacks[0];
		const pushed = [];
		dataStreamCb(
			{ __leo_delete__: 'id', __leo_delete_id__: 42 },
			() => {},
			obj => pushed.push(obj)
		);
		expect(pushed).to.deep.equal([{ __leo_delete__: 'id', __leo_delete_id__: 42 }]);
	});

	it('issues a dim soft-close UPDATE for __leo_delete__ records before MERGE', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		const p = callImportDimension(ctx.dwClient, 'd_account', ['retailer_id']);
		await new Promise(setImmediate);
		// Inject a delete record by calling the dataStream callback before the pipeline completes
		ctx.throughCallbacks[0](
			{ __leo_delete__: 'retailer_id', __leo_delete_id__: 42 },
			() => {},
			() => {}
		);
		ctx.completePipeline();
		await p;
		const updateQuery = ctx.queryHistory.find(q => q.startsWith('UPDATE'));
		expect(updateQuery, 'expected UPDATE query for dim soft-close').to.exist;
		expect(updateQuery).to.include('`_enddate`');
		expect(updateQuery).to.include('`retailer_id`');
		expect(updateQuery).to.include('42');
		expect(updateQuery).to.include('`_current` = true');
	});

	it('flushDimDeletes error propagates and skips MERGE', async () => {
		const ctx = setup({ tableFields: dimTableFields, failDimDeleteUpdate: true });
		const p = callImportDimension(ctx.dwClient, 'd_account', ['retailer_id']);
		await new Promise(setImmediate);
		ctx.throughCallbacks[0](
			{ __leo_delete__: 'retailer_id', __leo_delete_id__: 42 },
			() => {},
			() => {}
		);
		ctx.completePipeline();
		let caught;
		try {
			await p;
		} catch (e) {
			caught = e;
		}
		expect(caught, 'expected error from UPDATE failure').to.exist;
		expect(caught.message).to.equal('UPDATE failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('skips flushDimDeletes UPDATE when __leo_delete__ names a non-existent column', async () => {
		const ctx = setup({ tableFields: dimTableFields });
		const p = callImportDimension(ctx.dwClient, 'd_account', ['retailer_id']);
		await new Promise(setImmediate);
		ctx.throughCallbacks[0](
			{ __leo_delete__: 'bogus_column', __leo_delete_id__: 99 },
			() => {},
			() => {}
		);
		ctx.completePipeline();
		await p;
		expect(ctx.queryHistory.find(q => q.startsWith('UPDATE'))).to.not.exist;
	});

	it('returns staging cnt as result.count (single NK — pruneCol path)', async () => {
		const ctx = setup({ tableFields: dimTableFields, cnt: 42 });
		const result = await runToCompletion(ctx, 'd_account', ['retailer_id']);
		expect(result).to.exist;
		expect(result.count).to.equal(42);
	});

	it('returns staging cnt as result.count (composite NK — COUNT path)', async () => {
		const ctx = setup({ tableFields: dimTableFields, cnt: 17 });
		const result = await runToCompletion(ctx, 'd_account', ['retailer_id', 'name']);
		expect(result).to.exist;
		expect(result.count).to.equal(17);
	});

	it('propagates a MIN query error and skips MERGE', async () => {
		const ctx = setup({ tableFields: dimTableFields, failMinQuery: true });
		let caught;
		try {
			await runToCompletion(ctx, 'd_account', ['retailer_id']);
		} catch (e) {
			caught = e;
		}
		expect(caught, 'expected an error from MIN query').to.exist;
		expect(caught.message).to.equal('MIN query failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('propagates a COUNT query error and skips MERGE (composite NK path)', async () => {
		const ctx = setup({ tableFields: dimTableFields, failCountQuery: true });
		let caught;
		try {
			await runToCompletion(ctx, 'd_account', ['retailer_id', 'name']);
		} catch (e) {
			caught = e;
		}
		expect(caught, 'expected an error from COUNT query').to.exist;
		expect(caught.message).to.equal('COUNT query failed');
		expect(ctx.queryHistory.find(q => q.startsWith('MERGE INTO'))).to.not.exist;
	});

	it('enrichedStream stamps auditdate and computes sk, but does NOT set _deleted', async () => {
		const ctx = setup({
			tableFields: [
				...dimTableFields,
				{ column_name: '_id', data_type: 'BIGINT' },
			],
		});
		const tableDef = {
			structure: {
				'_id': 'sk',
				'retailer_id': { nk: true, type: 'bigint' },
				'name': { type: 'varchar(255)' },
			},
		};
		await runToCompletion(ctx, 'd_account', ['retailer_id'], tableDef);
		// throughCallbacks[1] is the enrichedStream (registered inside stageToS3 after describeTable)
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ retailer_id: 99, name: 'Rithum' }, (err, obj) => { enriched = obj; });
		expect(enriched.retailer_id).to.equal(99);
		expect(enriched.name).to.equal('Rithum');
		expect(enriched._auditdate).to.equal('2026-05-27T00:00:00');
		expect(enriched).to.not.have.property('_deleted');
		// _id is the sk column → fingerprint64 of natural-key values
		expect(typeof enriched._id).to.equal('string');
		expect(enriched._id).to.match(/^-?\d+$/);
	});

	it('enrichedStream computes entity FK hash for dimension field in dim table', async () => {
		const ctx = setup({
			tableFields: [
				...dimTableFields,
				{ column_name: 'account_id', data_type: 'INT' },
				{ column_name: 'd_account', data_type: 'BIGINT' },
			],
		});
		const tableDef = {
			structure: {
				'retailer_id': { nk: true, type: 'bigint' },
				'account_id': { type: 'integer', dimension: 'd_account' },
			},
		};
		await runToCompletion(ctx, 'd_item', ['retailer_id'], tableDef);
		const enrichedCb = ctx.throughCallbacks[1];
		let enriched;
		enrichedCb({ retailer_id: 1, account_id: 7 }, (err, obj) => { enriched = obj; });
		const fingerprint64 = require('../../lib/surrogate_key.js');
		expect(enriched.d_account).to.equal(fingerprint64([7]));
	});
});

'use strict';
/**
 * importDimension delete ordering (D3, PR #249).
 *
 * A dimension row created and deleted in one batch must land populated AND closed. That
 * requires the close for a staged ('id'-keyed) marker to run AFTER the merge inserts the
 * row — on the merge's own connection, inside its transaction (a pool-connection close
 * would wait on the transaction's locks). Closes for markers on any other column (a
 * parent FK) must keep running BEFORE the merge, because the same batch may write
 * replacement rows under the same value.
 *
 * These tests drive the real importDimension with a fake connection that records every
 * query, which connection ran it, and in what order. No database is involved: the
 * assertions are about ordering and targeting, which is exactly what the fix changes.
 */
const assert = require('assert').strict;
const { Readable, Writable, Transform, pipeline } = require('stream');
const Module = require('module');
const path = require('path');

// ---- module interception: stub leo-sdk / leo-logger / connect.js before dwconnect loads
const fakeLogger = Object.assign(function() { return fakeLogger; }, {
	error() {}, info() {}, debug() {}, warn() {}, log() {}, sub() { return fakeLogger; },
});
const fakeLs = {
	through(fn) {
		return new Transform({
			objectMode: true,
			transform(obj, _enc, cb) {
				fn.call(this, obj, (err, data) => {
					if (err) return cb(err);
					if (data !== undefined && data !== null && data !== true) this.push(data);
					cb();
				}, d => this.push(d));
			},
		});
	},
	pipe(...args) {
		const cb = args.pop();
		pipeline(...args, cb);
	},
};
let currentFakeClient = null;
const connectPath = path.resolve(__dirname, '../../lib/connect.js');
const origLoad = Module._load;
Module._load = function(request, parent) {
	if (request === 'leo-sdk') return { streams: fakeLs };
	if (request === 'leo-logger') return fakeLogger;
	if (parent && parent.filename && parent.filename.endsWith(`lib${path.sep}dwconnect.js`) &&
		(request === './connect.js' || path.resolve(path.dirname(parent.filename), request) === connectPath)) {
		return () => currentFakeClient;
	}
	return origLoad.apply(this, arguments);
};
const dwconnect = require('../../lib/dwconnect.js');
Module._load = origLoad;

// ---- fake client/connection
function makeFakeClient() {
	const log = []; // { sql, via: 'pool' | 'txn' }
	const staged = [];
	const respond = (sql) => {
		if (/v_dist_sort_key/i.test(sql)) return [{ sortkey: null, sortkeytype: null }];
		if (/as minid/i.test(sql)) return [{ minid: null, cnt: staged.length }];
		if (/as maxid/i.test(sql)) return [{ maxid: 10000 }];
		return [];
	};
	const record = (via) => (sql, paramsOrDone, maybeDone) => {
		const done = typeof paramsOrDone === 'function' ? paramsOrDone : maybeDone;
		log.push({ sql: String(sql).replace(/\s+/g, ' ').trim(), via });
		setImmediate(() => done(null, respond(String(sql))));
	};
	const connection = { query: record('txn'), release() {} };
	const client = {
		log,
		staged,
		query: record('pool'),
		connect: () => Promise.resolve(connection),
		describeTable: () => Promise.resolve(
			['id', 'parent_id', 'name', '_auditdate', '_startdate', '_enddate', '_current', 'sk_id']
				.map(c => ({ column_name: c }))
		),
		getSchemaCache: () => client._schema,
		_schema: {
			'public.d_test': ['id', 'parent_id', 'name', '_auditdate', '_startdate', '_enddate', '_current', 'sk_id']
				.map(c => ({ column_name: c })),
		},
		streamToTable: () => new Writable({
			objectMode: true,
			write(obj, _enc, cb) { staged.push(obj); cb(); },
		}),
		streamToTableFromS3: () => new Writable({
			objectMode: true,
			write(obj, _enc, cb) { staged.push(obj); cb(); },
		}),
		escapeValueNoToLower: v => `'${v}'`,
		auditdate: "'2026-10-19'",
	};
	return client;
}

function runImportDimension(config, rows) {
	return new Promise((resolve, reject) => {
		currentFakeClient = makeFakeClient();
		const client = dwconnect(config, {});
		const fake = currentFakeClient;
		client.importDimension(
			Readable.from(rows, { objectMode: true }),
			'd_test', 'sk_id', 'id', { 2: [], 3: [], 6: {} },
			(err) => err ? reject(err) : resolve(fake)
		);
	});
}

const isClose = e => /update public\.d_test set _enddate/i.test(e.sql);
const isInsert = e => /^insert into public\.d_test/i.test(e.sql);
const isCommit = e => /^commit$/i.test(e.sql);
const idx = (log, pred) => log.findIndex(pred);

const bypassConfig = { version: 'postgres', hashedSurrogateKeys: false, bypassSlowlyChangingDimensions: true };
const scdConfig = { version: 'postgres', hashedSurrogateKeys: false, bypassSlowlyChangingDimensions: false };

describe('importDimension delete ordering (D3, PR #249)', () => {

	it('created+deleted in one batch: data is staged, close runs on the transaction after the insert, before commit', async () => {
		const fake = await runImportDimension(bypassConfig, [
			{ id: 7, name: 'x', __leo_delete__: 'id', __leo_delete_id__: 7 },
		]);
		assert.equal(fake.staged.length, 1, 'the marker-with-data row must reach staging');
		assert.equal(fake.staged[0].id, 7);
		const closes = fake.log.filter(isClose);
		assert.equal(closes.length, 1, `expected exactly one close, got: ${JSON.stringify(closes)}`);
		assert.equal(closes[0].via, 'txn', 'the staged-delete close must run on the transaction connection, not the pool');
		assert.match(closes[0].sql, /where id in \('7'\) and _current = true/i);
		const log = fake.log;
		assert.ok(idx(log, isInsert) >= 0, 'merge insert must run');
		assert.ok(idx(log, isClose) > idx(log, isInsert), 'close must run after the merge insert');
		assert.ok(idx(log, isClose) < idx(log, isCommit), 'close must run inside the transaction, before commit');
	});

	it('parent-FK delete with a same-batch write: close stays on the pool, before the merge, and only for the FK', async () => {
		const fake = await runImportDimension(bypassConfig, [
			{ __leo_delete__: 'parent_id', __leo_delete_id__: 5 },
			{ id: 9, parent_id: 5, name: 'new' },
		]);
		assert.equal(fake.staged.length, 1, 'only the write is staged; FK markers are not');
		assert.equal(fake.staged[0].id, 9);
		const closes = fake.log.filter(isClose);
		assert.equal(closes.length, 1);
		assert.equal(closes[0].via, 'pool', 'FK-marker close keeps running outside the merge transaction, as today');
		assert.match(closes[0].sql, /where parent_id in \('5'\)/i);
		assert.ok(idx(fake.log, isClose) < idx(fake.log, isInsert), 'FK close must run before the merge insert');
	});

	it('write-only batch: no close runs at all', async () => {
		const fake = await runImportDimension(bypassConfig, [
			{ id: 11, name: 'only-write' },
		]);
		assert.equal(fake.log.filter(isClose).length, 0);
		assert.equal(fake.staged.length, 1);
	});

	it('deferral beats the mid-batch flush threshold: staged deletes close after the insert, chunked at deleteFlushCount', async () => {
		const fake = await runImportDimension({ ...bypassConfig, deleteFlushCount: 2 }, [
			{ id: 1, __leo_delete__: 'id', __leo_delete_id__: 1 },
			{ id: 2, __leo_delete__: 'id', __leo_delete_id__: 2 },
			{ id: 3, __leo_delete__: 'id', __leo_delete_id__: 3 },
		]);
		const closes = fake.log.filter(isClose);
		assert.equal(closes.length, 2, 'the forced flush must chunk ids at deleteFlushCount, never one unbounded IN list');
		assert.match(closes[0].sql, /where id in \('1','2'\)/i);
		assert.match(closes[1].sql, /where id in \('3'\)/i);
		const log = fake.log;
		closes.forEach(c => assert.equal(c.via, 'txn'));
		assert.ok(log.indexOf(closes[0]) > idx(log, isInsert), 'first chunk after the merge insert');
		assert.ok(log.indexOf(closes[1]) < idx(log, isCommit), 'last chunk before commit');
	});

	it('SCD path gets the same ordering: staged-delete close after the merge, before commit, on the transaction', async () => {
		const fake = await runImportDimension(scdConfig, [
			{ id: 7, name: 'x', __leo_delete__: 'id', __leo_delete_id__: 7 },
		]);
		const closes = fake.log.filter(isClose);
		assert.equal(closes.length, 1);
		assert.equal(closes[0].via, 'txn');
		const log = fake.log;
		assert.ok(idx(log, isClose) > idx(log, isInsert), 'close after the SCD insert');
		assert.ok(idx(log, isClose) < idx(log, isCommit), 'close before commit');
	});
});

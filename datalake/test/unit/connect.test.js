'use strict';

const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// Stub for @databricks/sql
function makeDatabricksStub(overrides) {
	const sessionStub = Object.assign({
		executeStatement: sinon.stub().resolves({
			fetchAll: sinon.stub().resolves([]),
			close: sinon.stub().resolves(),
		}),
		close: sinon.stub().resolves(),
	}, overrides && overrides.session);

	const clientStub = Object.assign({
		connect: sinon.stub().resolves(),
		openSession: sinon.stub().resolves(sessionStub),
		close: sinon.stub().resolves(),
	}, overrides && overrides.client);

	return {
		DBSQLClient: sinon.stub().returns(clientStub),
		_session: sessionStub,
		_client: clientStub,
	};
}

// generic-pool stub — lightweight pool that immediately creates/releases/destroys
// without any async complexity. Tracks borrow accounting for unit tests.
function makePoolStub(overrides) {
	let borrowed = 0;
	let pending = 0;
	let drainCalled = false;
	let clearCalled = false;
	const stub = Object.assign({
		acquire: sinon.stub(),
		release: sinon.stub().resolves(),
		destroy: sinon.stub().resolves(),
		drain: sinon.stub().callsFake(() => { drainCalled = true; return Promise.resolve(); }),
		clear: sinon.stub().callsFake(() => { clearCalled = true; return Promise.resolve(); }),
		get borrowed() { return borrowed; },
		get pending() { return pending; },
		_setBorrowed: v => { borrowed = v; },
		_setPending: v => { pending = v; },
		_drainCalled: () => drainCalled,
		_clearCalled: () => clearCalled,
	}, overrides);
	return stub;
}

function makeGenericPoolStub(poolStub) {
	return {
		createPool: sinon.stub().returns(poolStub),
	};
}

function makeConnectFactory(databricksStub, genericPoolStub) {
	lsStreamsStub = {
		pipeline: sinon.stub(),
		through: sinon.stub(),
		toS3: sinon.stub(),
	};
	csvStub = { createWriteStream: sinon.stub() };
	return proxyquire('../../lib/connect.js', {
		'@databricks/sql': databricksStub,
		'generic-pool': genericPoolStub,
		'leo-logger': () => ({ info: () => {}, debug: () => {}, error: () => {} }),
		'leo-sdk': { streams: lsStreamsStub },
		'fast-csv': csvStub,
	});
}

let lsStreamsStub, csvStub;

describe('connect.js', () => {
	let connectFactory, databricksStub, poolStub, genericPoolStub;

	beforeEach(() => {
		databricksStub = makeDatabricksStub();
		poolStub = makePoolStub();
		genericPoolStub = makeGenericPoolStub(poolStub);
		connectFactory = makeConnectFactory(databricksStub, genericPoolStub);
	});

	describe('interface surface', () => {
		it('exposes all required methods', () => {
			const client = connectFactory({ host: 'test.azuredatabricks.net', path: '/sql/1', token: 'tok', catalog: 'cat', schema: 'sch' });
			expect(client.connect).to.be.a('function');
			expect(client.query).to.be.a('function');
			expect(client.disconnect).to.be.a('function');
			expect(client.end).to.be.a('function');
			expect(client.release).to.be.a('function');
			expect(client.describeTable).to.be.a('function');
			expect(client.describeTables).to.be.a('function');
			expect(client.getSchemaCache).to.be.a('function');
			expect(client.setSchemaCache).to.be.a('function');
			expect(client.clearSchemaCache).to.be.a('function');
			expect(client.streamToTableFromS3).to.be.a('function');
			expect(client.escapeId).to.be.a('function');
			expect(client.escape).to.be.a('function');
		});
	});

	describe('factory construction — no network at construct time', () => {
		it('does NOT call sqlClient.connect() when the factory is called', () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			expect(databricksStub._client.connect.called,
				'sqlClient.connect should not fire at factory time').to.be.false;
		});

		it('creates DBSQLClient synchronously', () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			expect(databricksStub.DBSQLClient.calledOnce).to.be.true;
		});
	});

	describe('config normalization', () => {
		it('poolMax defaults to 10', () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const poolOpts = genericPoolStub.createPool.firstCall.args[1];
			expect(poolOpts.max).to.equal(10);
		});

		it('poolMax is clamped to [1, 50]', () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's', poolMax: 0 });
			expect(genericPoolStub.createPool.firstCall.args[1].max).to.equal(1);

			genericPoolStub.createPool.resetHistory();
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's', poolMax: 999 });
			expect(genericPoolStub.createPool.firstCall.args[1].max).to.equal(50);
		});

		it('statementTimeoutSeconds defaults to 600', async () => {
			poolStub.acquire.resolves({ dead: false, query: sinon.stub().callsFake((s, p, cb) => cb(null, [])), release: () => {} });
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			// Trigger pool.acquire() so the factory.create fires and we can inspect openSession args
			databricksStub._client.openSession = sinon.stub().callsFake(async (opts) => {
				expect(opts.initialParameters.STATEMENT_TIMEOUT).to.equal('600');
				return databricksStub._session;
			});
			// Call the pool factory's create directly (bypassing the stub)
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			await createFn();
		});

		it('statementTimeoutSeconds is floored to 5', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's', statementTimeoutSeconds: 0 });
			databricksStub._client.openSession = sinon.stub().callsFake(async (opts) => {
				expect(opts.initialParameters.STATEMENT_TIMEOUT).to.equal('5');
				return databricksStub._session;
			});
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			await createFn();
		});

		it('statementTimeoutSeconds is capped at 1800', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's', statementTimeoutSeconds: 99999 });
			databricksStub._client.openSession = sinon.stub().callsFake(async (opts) => {
				expect(opts.initialParameters.STATEMENT_TIMEOUT).to.equal('1800');
				return databricksStub._session;
			});
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			await createFn();
		});

		it('poolMax falls back to 10 when given a non-numeric value', () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's', poolMax: 'notanumber' });
			expect(genericPoolStub.createPool.firstCall.args[1].max).to.equal(10);
		});

		it('socketTimeout is pinned above statementTimeoutSeconds * 1000', () => {
			// With default statementTimeoutSeconds=600, socketTimeout = (600+120)*1000 = 720000
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			// Trigger ensureConnected inside factory.create
			void createFn();
			// sqlClient.connect is called with socketTimeout
			expect(databricksStub._client.connect.called).to.be.true;
			const connectArg = databricksStub._client.connect.firstCall.args[0];
			expect(connectArg.socketTimeout).to.equal(720000);
			// socketTimeout > statementTimeoutSeconds * 1000
			expect(connectArg.socketTimeout).to.be.above(600 * 1000);
		});
	});

	describe('memoized connect — single sqlClient.connect() under N-way burst', () => {
		it('fires sqlClient.connect exactly once even when factory.create is called N times', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			// Simulate 5 concurrent factory.create calls
			await Promise.all([createFn(), createFn(), createFn(), createFn(), createFn()]);
			expect(databricksStub._client.connect.callCount,
				'sqlClient.connect must be called exactly once').to.equal(1);
		});
	});

	describe('session params', () => {
		it('passes ansi_mode=false, infer_timestamp_ntz_type=true, timezone=UTC, STATEMENT_TIMEOUT', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			await createFn();
			const params = databricksStub._client.openSession.firstCall.args[0].initialParameters;
			expect(params.ansi_mode).to.equal('false');
			expect(params.infer_timestamp_ntz_type).to.equal('true');
			expect(params.timezone).to.equal('UTC');
			expect(params.STATEMENT_TIMEOUT).to.equal('600');
		});

		it('opens session with initialCatalog and initialSchema', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'mycat', schema: 'mysch' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			await createFn();
			const sessionArgs = databricksStub._client.openSession.firstCall.args[0];
			expect(sessionArgs.initialCatalog).to.equal('mycat');
			expect(sessionArgs.initialSchema).to.equal('mysch');
		});
	});

	describe('query() — pool acquire/release/destroy routing', () => {
		it('calls pool.acquire() and pool.release() on success', (done) => {
			const wrapper = {
				dead: false,
				query: sinon.stub().callsFake((s, p, cb) => cb(null, [{ id: 1 }], [])),
				release: () => {},
			};
			poolStub.acquire.resolves(wrapper);

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.query('SELECT 1', [], (err, rows) => {
				expect(err).to.be.null;
				expect(rows).to.deep.equal([{ id: 1 }]);
				expect(poolStub.acquire.calledOnce).to.be.true;
				expect(poolStub.release.calledWith(wrapper)).to.be.true;
				expect(poolStub.destroy.called).to.be.false;
				done();
			});
		});

		it('sets wrapper.dead=true and calls pool.destroy() on connection-class error', (done) => {
			const connErr = new Error('ECONNRESET');
			connErr.code = 'ECONNRESET';
			const wrapper = {
				dead: false,
				query: sinon.stub().callsFake((s, p, cb) => cb(connErr)),
				release: () => {},
			};
			poolStub.acquire.resolves(wrapper);

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.query('SELECT 1', [], (err) => {
				expect(err).to.equal(connErr);
				expect(wrapper.dead).to.be.true;
				expect(poolStub.destroy.calledWith(wrapper)).to.be.true;
				expect(poolStub.release.called).to.be.false;
				done();
			});
		});

		it('calls pool.release() (not destroy) on query-class SQL error', (done) => {
			const sqlErr = new Error('[PARSE_SYNTAX_ERROR] Syntax error at line 1');
			const wrapper = {
				dead: false,
				query: sinon.stub().callsFake((s, p, cb) => cb(sqlErr)),
				release: () => {},
			};
			poolStub.acquire.resolves(wrapper);

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.query('BAD SQL', [], (err) => {
				expect(err).to.equal(sqlErr);
				expect(wrapper.dead).to.be.false;
				expect(poolStub.release.calledWith(wrapper)).to.be.true;
				expect(poolStub.destroy.called).to.be.false;
				done();
			});
		});

		it('propagates acquire() rejection via callback', (done) => {
			poolStub.acquire.rejects(new Error('acquire timeout'));

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.query('SELECT 1', [], (err) => {
				expect(err.message).to.equal('acquire timeout');
				done();
			});
		});
	});

	describe('createSessionClient.query() — inRowMode', () => {
		async function makeWrapper(fetchAllResult) {
			const operation = {
				fetchAll: sinon.stub().resolves(fetchAllResult),
				close: sinon.stub().resolves(),
			};
			databricksStub._session.executeStatement = sinon.stub().resolves(operation);
			// connectFactory must be called first so createPool registers the factory
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			return createFn();
		}

		it('returns object rows by default (no opts)', async () => {
			const rows = [{ a: 1, b: 2 }, { a: 3, b: 4 }];
			const wrapper = await makeWrapper(rows);
			await new Promise((resolve, reject) => {
				wrapper.query('SELECT 1', [], (err, result, fields) => {
					if (err) return reject(err);
					expect(result).to.deep.equal(rows);
					expect(fields.map(f => f.name)).to.deep.equal(['a', 'b']);
					resolve();
				});
			});
		});

		it('returns array rows when inRowMode:true', async () => {
			const rows = [{ a: 1, b: 2 }, { a: 3, b: 4 }];
			const wrapper = await makeWrapper(rows);
			await new Promise((resolve, reject) => {
				wrapper.query('SELECT 1', [], (err, result, fields) => {
					if (err) return reject(err);
					expect(result).to.deep.equal([[1, 2], [3, 4]]);
					expect(fields.map(f => f.name)).to.deep.equal(['a', 'b']);
					resolve();
				}, { inRowMode: true });
			});
		});

		it('threads opts through client.query() 3-arg call form (callback second)', (done) => {
			const rows = [{ x: 10 }, { x: 20 }];
			const operation = {
				fetchAll: sinon.stub().resolves(rows),
				close: sinon.stub().resolves(),
			};
			databricksStub._session.executeStatement = sinon.stub().resolves(operation);
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;

			createFn().then(wrapper => {
				poolStub.acquire.resolves(wrapper);
				client.query('SELECT x', (err, result, fields) => {
					expect(err).to.be.null;
					expect(result).to.deep.equal([[10], [20]]);
					expect(fields.map(f => f.name)).to.deep.equal(['x']);
					done();
				}, { inRowMode: true });
			}).catch(done);
		});

		it('returns empty array without error when fetchAll returns []', async () => {
			const wrapper = await makeWrapper([]);
			await new Promise((resolve, reject) => {
				wrapper.query('SELECT 1', [], (err, result, fields) => {
					if (err) return reject(err);
					expect(result).to.deep.equal([]);
					expect(fields).to.deep.equal([]);
					resolve();
				}, { inRowMode: true });
			});
		});

		it('handles 3-arg call (callback as second arg, no params array)', async () => {
			const rows = [{ a: 1 }];
			const op = { fetchAll: sinon.stub().resolves(rows), close: sinon.stub().resolves() };
			databricksStub._session.executeStatement = sinon.stub().resolves(op);
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			const wrapper = await createFn();
			await new Promise((resolve, reject) => {
				wrapper.query('SELECT 1', (err, result) => {
					if (err) return reject(err);
					expect(result).to.deep.equal(rows);
					expect(databricksStub._session.executeStatement.firstCall.args[1]).to.deep.equal({});
					resolve();
				});
			});
		});

		it('calls cb(err) when executeStatement rejects', async () => {
			const execError = new Error('Session error');
			databricksStub._session.executeStatement = sinon.stub().rejects(execError);
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			const wrapper = await createFn();
			const err = await new Promise(resolve => {
				wrapper.query('SELECT 1', [], resolve);
			});
			expect(err).to.equal(execError);
		});
	});

	describe('pool.validate() — dead-flag check', () => {
		it('validate returns true when dead=false', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const validateFn = genericPoolStub.createPool.firstCall.args[0].validate;
			const result = await validateFn({ dead: false });
			expect(result).to.be.true;
		});

		it('validate returns false when dead=true', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const validateFn = genericPoolStub.createPool.firstCall.args[0].validate;
			const result = await validateFn({ dead: true });
			expect(result).to.be.false;
		});
	});

	describe('disconnect() / end() — drain, clear, close', () => {
		it('disconnect() calls pool.drain(), pool.clear(), sqlClient.close()', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			await client.disconnect();
			expect(poolStub.drain.calledOnce).to.be.true;
			expect(poolStub.clear.calledOnce).to.be.true;
			expect(databricksStub._client.close.calledOnce).to.be.true;
		});

		it('end() calls pool.drain(), pool.clear(), sqlClient.close()', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			await client.end();
			expect(poolStub.drain.calledOnce).to.be.true;
			expect(poolStub.clear.calledOnce).to.be.true;
			expect(databricksStub._client.close.calledOnce).to.be.true;
		});
	});

	describe('pool factory.destroy() — closes the underlying session', () => {
		it('destroy() calls wrapper._session.close()', async () => {
			connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const createFn = genericPoolStub.createPool.firstCall.args[0].create;
			const destroyFn = genericPoolStub.createPool.firstCall.args[0].destroy;
			const wrapper = await createFn();
			await destroyFn(wrapper);
			expect(databricksStub._session.close.calledOnce).to.be.true;
		});
	});

	describe('schema cache', () => {
		it('round-trips set/get', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			const data = { 's.my_table': [{ column_name: 'id', data_type: 'INT' }] };
			client.setSchemaCache(data);
			expect(client.getSchemaCache()).to.deep.equal(data);
		});

		it('clearSchemaCache empties the cache', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.setSchemaCache({ 's.t': [{ column_name: 'x' }] });
			client.clearSchemaCache();
			expect(Object.keys(client.getSchemaCache())).to.have.length(0);
		});
	});

	describe('escapeId', () => {
		it('lowercases and wraps in backticks', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escapeId('OrderId')).to.equal('`orderid`');
			expect(client.escapeId('retailer_account_id')).to.equal('`retailer_account_id`');
			expect(client.escapeId('MixedCase')).to.equal('`mixedcase`');
		});

		it('strips existing backticks', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escapeId('bad`name')).to.equal('`badname`');
		});
	});

	describe('TIMESTAMP_NTZ normalization helpers', () => {
		// Pull the unstubbed helpers — they're pure functions and don't touch the
		// Databricks/leo-sdk stubs above. Using require directly avoids the
		// proxyquire shim, which doesn't proxy named-export augmentations.
		const realConnect = require('../../lib/connect.js');

		describe('stripTimestampOffset', () => {
			it('strips trailing Z', () => {
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00Z')).to.equal('2026-03-15T14:30:00');
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00.123Z')).to.equal('2026-03-15T14:30:00.123');
			});

			it('strips ±HH:MM offsets', () => {
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00-08:00')).to.equal('2026-03-15T14:30:00');
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00+05:30')).to.equal('2026-03-15T14:30:00');
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00.250+00:00')).to.equal('2026-03-15T14:30:00.250');
			});

			it('strips ±HHMM offsets (no colon)', () => {
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00-0800')).to.equal('2026-03-15T14:30:00');
			});

			it('accepts space separator between date and time', () => {
				expect(realConnect.stripTimestampOffset('2026-03-15 14:30:00Z')).to.equal('2026-03-15 14:30:00');
			});

			it('leaves naked ISO unchanged', () => {
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00')).to.equal('2026-03-15T14:30:00');
				expect(realConnect.stripTimestampOffset('2026-03-15T14:30:00.123')).to.equal('2026-03-15T14:30:00.123');
			});

			it('leaves non-string and non-matching values unchanged', () => {
				expect(realConnect.stripTimestampOffset(null)).to.equal(null);
				expect(realConnect.stripTimestampOffset(undefined)).to.equal(undefined);
				expect(realConnect.stripTimestampOffset(42)).to.equal(42);
				expect(realConnect.stripTimestampOffset('not a timestamp')).to.equal('not a timestamp');
				// Pure date — different shape, must pass through; DATE columns are
				// not NTZ anyway, but defense in depth.
				expect(realConnect.stripTimestampOffset('2026-03-15')).to.equal('2026-03-15');
			});

			it('coerces Date instances to ISO and strips the Z', () => {
				// Date is inherently a UTC instant — `new Date(0)` is the epoch
				// regardless of host TZ. The strip should produce the naked UTC
				// wall-clock, not the local one.
				expect(realConnect.stripTimestampOffset(new Date(Date.UTC(2026, 2, 15, 14, 30, 0))))
					.to.equal('2026-03-15T14:30:00.000');
				expect(realConnect.stripTimestampOffset(new Date(Date.UTC(2026, 0, 1, 0, 0, 0, 250))))
					.to.equal('2026-01-01T00:00:00.250');
			});

			it('passes invalid Date instances through unchanged', () => {
				const bad = new Date('not-a-real-date');
				expect(realConnect.stripTimestampOffset(bad)).to.equal(bad);
			});
		});

		describe('stagingS3Path', () => {
			it('builds {bucket, key, uri} with correct structure and random suffix', () => {
				const p = realConnect.stagingS3Path('bkt', 'prefix/sub', 'f_order', "'2026-03-15T14:30:00'");
				expect(p.bucket).to.equal('bkt');
				// key: {prefix}/{table}/{auditdate}-{8hex}.csv
				expect(p.key).to.match(/^prefix\/sub\/f_order\/2026-03-15T14-30-00-[0-9a-f]{8}\.csv$/);
				expect(p.uri).to.match(/^s3:\/\/bkt\/prefix\/sub\/f_order\/2026-03-15T14-30-00-[0-9a-f]{8}\.csv$/);
			});

			it('strips a trailing slash from the prefix', () => {
				const p = realConnect.stagingS3Path('bkt', 'prefix/sub/', 't', "'2026-01-01T00:00:00'");
				expect(p.key).to.match(/^prefix\/sub\/t\/2026-01-01T00-00-00-[0-9a-f]{8}\.csv$/);
			});

			it('strips surrounding single quotes and replaces colons in the auditdate', () => {
				const p = realConnect.stagingS3Path('b', 'p', 't', "'2026-03-15T14:30:00.123Z'");
				// colons → dashes; quotes stripped; rest preserved; random suffix appended
				expect(p.key).to.match(/^p\/t\/2026-03-15T14-30-00\.123Z-[0-9a-f]{8}\.csv$/);
			});

			it('two parallel callers with different tables produce distinct paths', () => {
				const auditdate = "'2026-03-15T14:30:00'";
				const a = realConnect.stagingS3Path('b', 'p', 'f_order', auditdate);
				const b = realConnect.stagingS3Path('b', 'p', 'f_order_item', auditdate);
				expect(a.key).to.not.equal(b.key);
				expect(a.uri).to.not.equal(b.uri);
			});

			it('two parallel callers with the same table produce distinct paths', () => {
				const auditdate = "'2026-03-15T14:30:00'";
				const a = realConnect.stagingS3Path('b', 'p', 'f_order', auditdate);
				const b = realConnect.stagingS3Path('b', 'p', 'f_order', auditdate);
				expect(a.key).to.not.equal(b.key);
				expect(a.uri).to.not.equal(b.uri);
			});

			it('throws when bucket or prefix is missing', () => {
				expect(() => realConnect.stagingS3Path(null, 'p', 't', "'x'")).to.throw(/unresolved/);
				expect(() => realConnect.stagingS3Path('b', null, 't', "'x'")).to.throw(/unresolved/);
			});
		});

		describe('isNtzType', () => {
			it('matches TIMESTAMP_NTZ in any case', () => {
				expect(realConnect.isNtzType('TIMESTAMP_NTZ')).to.equal(true);
				expect(realConnect.isNtzType('timestamp_ntz')).to.equal(true);
				expect(realConnect.isNtzType(' Timestamp_Ntz ')).to.equal(true);
			});

			it('rejects zone-aware TIMESTAMP and other types', () => {
				// TIMESTAMP (zone-aware in Databricks) MUST NOT be stripped — the
				// offset is meaningful for that type.
				expect(realConnect.isNtzType('TIMESTAMP')).to.equal(false);
				expect(realConnect.isNtzType('STRING')).to.equal(false);
				expect(realConnect.isNtzType('DATE')).to.equal(false);
				expect(realConnect.isNtzType(null)).to.equal(false);
				expect(realConnect.isNtzType(undefined)).to.equal(false);
			});
		});

		describe('isConnectionError', () => {
			it('returns true for FetchError', () => {
				const e = new Error('fetch failed');
				e.name = 'FetchError';
				expect(realConnect.isConnectionError(e)).to.be.true;
			});

			it('returns true for ECONNRESET', () => {
				const e = new Error('socket reset');
				e.code = 'ECONNRESET';
				expect(realConnect.isConnectionError(e)).to.be.true;
			});

			it('returns true for socket hang up message', () => {
				expect(realConnect.isConnectionError(new Error('socket hang up'))).to.be.true;
			});

			it('returns true for session closed message', () => {
				expect(realConnect.isConnectionError(new Error('Session is closed'))).to.be.true;
			});

			it('returns false for known SQL compilation errors', () => {
				expect(realConnect.isConnectionError(new Error('[PARSE_SYNTAX_ERROR] bad sql'))).to.be.false;
				expect(realConnect.isConnectionError(new Error('SQL compilation error: table not found'))).to.be.false;
				expect(realConnect.isConnectionError(new Error('[PERMISSION_DENIED] access denied'))).to.be.false;
			});

			it('returns false for null/undefined', () => {
				expect(realConnect.isConnectionError(null)).to.be.false;
				expect(realConnect.isConnectionError(undefined)).to.be.false;
			});

			it('returns true for transport error and connection closed messages', () => {
				expect(realConnect.isConnectionError(new Error('transport error occurred'))).to.be.true;
				expect(realConnect.isConnectionError(new Error('connection closed by server'))).to.be.true;
			});
		});
	});

	describe('connect() — acquires from pool', () => {
		it('supports 3-arg callback form (callback as second arg, no params)', (done) => {
			const wrapper = { dead: false, query: sinon.stub().callsFake((s, p, cb) => cb(null, [{ x: 1 }], [])), release: () => {} };
			poolStub.acquire.resolves(wrapper);
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.connect().then(conn => {
				conn.query('SELECT 1', (err, rows) => {
					expect(err).to.be.null;
					expect(rows).to.deep.equal([{ x: 1 }]);
					expect(wrapper.query.firstCall.args[1]).to.deep.equal([]);
					done();
				});
			}).catch(done);
		});

		it('calls pool.acquire() and returns a wrapper with working release()', async () => {
			const wrapper = { dead: false, query: sinon.stub(), release: () => {} };
			poolStub.acquire.resolves(wrapper);

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			const conn = await client.connect();
			expect(poolStub.acquire.calledOnce).to.be.true;
			// conn is a thin wrapper — not the raw pool resource — so release() works
			expect(conn).to.not.equal(wrapper);
			expect(conn.dead).to.equal(wrapper.dead);
			// release() on a healthy wrapper calls pool.release with the original reference
			conn.release();
			expect(poolStub.release.calledOnce).to.be.true;
			expect(poolStub.release.firstCall.args[0]).to.equal(wrapper);
			expect(poolStub.destroy.called).to.be.false;
		});

		it('marks wrapper dead and calls pool.destroy on release() after a connection-class error', (done) => {
			const wrapper = { dead: false, query: sinon.stub(), release: () => {} };
			poolStub.acquire.resolves(wrapper);
			// Simulate a connection-class error from the session query
			const connErr = new Error('socket hang up');
			wrapper.query.callsFake((_sql, _params, cb) => cb(connErr));

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.connect().then(conn => {
				conn.query('SELECT 1', [], (err) => {
					expect(err).to.equal(connErr);
					expect(wrapper.dead).to.be.true;
					// release() must destroy rather than return the broken wrapper
					conn.release();
					expect(poolStub.destroy.calledOnce).to.be.true;
					expect(poolStub.destroy.firstCall.args[0]).to.equal(wrapper);
					expect(poolStub.release.called).to.be.false;
					done();
				});
			}).catch(done);
		});

		it('calls pool.release (not destroy) on release() after a non-connection query error', (done) => {
			const wrapper = { dead: false, query: sinon.stub(), release: () => {} };
			poolStub.acquire.resolves(wrapper);
			// Simulate a query-class error (syntax error — not a connection failure)
			const queryErr = new Error('[Databricks][SQL] syntax error');
			wrapper.query.callsFake((_sql, _params, cb) => cb(queryErr));

			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.connect().then(conn => {
				conn.query('BAD SQL', [], (err) => {
					expect(err).to.equal(queryErr);
					expect(wrapper.dead).to.be.false;
					conn.release();
					expect(poolStub.release.calledOnce).to.be.true;
					expect(poolStub.destroy.called).to.be.false;
					done();
				});
			}).catch(done);
		});
	});

	describe('describeTable', () => {
		it('resolves from schema cache when populated', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			const cachedCols = [{ column_name: 'id', data_type: 'BIGINT' }];
			client.setSchemaCache({ 'sch.my_table': cachedCols });
			const result = await client.describeTable('my_table', 'sch');
			expect(result).to.deep.equal(cachedCols);
		});

		it('calls describeTables and resolves with the matching table columns', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			const rows = [{ table_name: 'my_table', column_name: 'id', data_type: 'BIGINT', numeric_precision: null, numeric_scale: null, is_nullable: 'YES' }];
			client.query = sinon.stub().callsFake((sql, params, cb) => cb(null, rows));
			const result = await client.describeTable('my_table', 'sch');
			expect(result).to.deep.equal(rows);
		});

		it('rejects with NO_SCHEMA_FOUND when table is absent from schema', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			client.query = sinon.stub().callsFake((sql, params, cb) => cb(null, []));
			let caught;
			try { await client.describeTable('missing_table', 'sch'); } catch (e) { caught = e; }
			expect(caught).to.equal('NO_SCHEMA_FOUND');
		});
	});

	describe('describeTables', () => {
		it('resolves from cache when schema cache has entries', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			const cached = { 'sch.t1': [{ column_name: 'id' }] };
			client.setSchemaCache(cached);
			const result = await client.describeTables('sch');
			expect(result).to.deep.equal(cached);
		});

		it('queries information_schema and groups columns by table_name', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			const rows = [
				{ table_name: 't1', column_name: 'id',   data_type: 'BIGINT', numeric_precision: null, numeric_scale: null },
				{ table_name: 't1', column_name: 'name', data_type: 'STRING', numeric_precision: null, numeric_scale: null },
				{ table_name: 't2', column_name: 'x',    data_type: 'INT',    numeric_precision: null, numeric_scale: null },
			];
			client.query = sinon.stub().callsFake((sql, params, cb) => cb(null, rows));
			const result = await client.describeTables('sch');
			expect(result['sch.t1']).to.have.length(2);
			expect(result['sch.t2']).to.have.length(1);
			expect(client.query.firstCall.args[0]).to.include('information_schema.columns');
			expect(client.query.firstCall.args[1]).to.deep.equal(['sch']);
		});

		it('propagates query error', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'cat', schema: 'sch' });
			const dbErr = new Error('permission denied');
			client.query = sinon.stub().callsFake((sql, params, cb) => cb(dbErr));
			let caught;
			try { await client.describeTables('sch'); } catch (e) { caught = e; }
			expect(caught).to.equal(dbErr);
		});
	});

	describe('escape / escapeValue / escapeValueNoToLower', () => {
		it('escape wraps in single quotes and escapes embedded single quotes', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escape('foo')).to.equal("'foo'");
			expect(client.escape("it's")).to.equal("'it\\'s'");
		});

		it('escape doubles backslashes before escaping quotes', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			// trailing backslash: value \ → SQL '\\' (ansi_mode=false: \\ → \)
			expect(client.escape('\\')).to.equal("'\\\\'" );
			// backslash in middle: foo\bar → SQL 'foo\\bar'
			expect(client.escape('foo\\bar')).to.equal("'foo\\\\bar'");
			// backslash immediately before quote: foo\'bar → SQL 'foo\\\'bar'
			expect(client.escape("foo\\'bar")).to.equal("'foo\\\\\\'bar'");
		});

		it('escape returns non-string values unchanged', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escape(42)).to.equal(42);
			expect(client.escape(null)).to.equal(null);
		});

		it('escapeValue lowercases and wraps in single quotes', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escapeValue('FooBar')).to.equal("'foobar'");
		});

		it('escapeValueNoToLower preserves case', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escapeValueNoToLower('FooBar')).to.equal("'FooBar'");
		});

		it('escapeValue and escapeValueNoToLower return non-string values unchanged', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(client.escapeValue(null)).to.equal(null);
			expect(client.escapeValue(42)).to.equal(42);
			expect(client.escapeValueNoToLower(null)).to.equal(null);
			expect(client.escapeValueNoToLower(0)).to.equal(0);
		});
	});

	describe('buildStagingSelect', () => {
		it('returns SELECT * FROM read_files with the schema string', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			const sql = client.buildStagingSelect('s3://bkt/key.csv', [{ name: 'id', type: 'BIGINT' }, { name: 'name', type: 'STRING' }]);
			expect(sql).to.include('SELECT * FROM read_files(');
			expect(sql).to.include("'s3://bkt/key.csv'");
			expect(sql).to.include('id BIGINT, name STRING');
		});

		it('excludes _rescued_data from the positional schema string', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			const sql = client.buildStagingSelect('s3://b/k', [{ name: 'id', type: 'BIGINT' }, { name: '_rescued_data', type: 'STRING' }]);
			// _rescued_data must not appear in the schema => '...' clause
			const schemaMatch = sql.match(/schema => '([^']+)'/);
			expect(schemaMatch).to.exist;
			expect(schemaMatch[1]).to.equal('id BIGINT');
			expect(schemaMatch[1]).to.not.include('_rescued_data');
		});

		it('sets sep to pipe, nullValue to \\\\N, and mode to PERMISSIVE', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			const sql = client.buildStagingSelect('s3://b/k', [{ name: 'x', type: 'INT' }]);
			expect(sql).to.include("sep => '|'");
			expect(sql).to.include("nullValue => '\\\\N'");
			expect(sql).to.include("mode => 'PERMISSIVE'");
		});
	});

	describe('stagingS3Path client wrapper', () => {
		it('delegates to module stagingS3Path using client.s3Bucket/s3Prefix', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.s3Bucket = 'mybkt';
			client.s3Prefix = 'myprefix';
			const path = client.stagingS3Path('my_table', "'2026-01-01T00:00:00'");
			expect(path.bucket).to.equal('mybkt');
			expect(path.key).to.match(/^myprefix\/my_table\/2026-01-01T00-00-00-[0-9a-f]{8}\.csv$/);
			expect(path.uri).to.match(/^s3:\/\/mybkt\//);
		});
	});

	describe('streamToTableFromS3 / streamToTable', () => {
		it('streamToTable throws not-implemented', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			expect(() => client.streamToTable()).to.throw(/not implemented/);
		});

		it('throws when columnDefs is missing', () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'bkt', s3Prefix: 'pre' });
			expect(() => client.streamToTableFromS3('t', {})).to.throw('columnDefs required');
		});

		it('calls csv.createWriteStream with pipe delimiter and ls.pipeline/toS3', () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'bkt', s3Prefix: 'pre' });
			const s3Path = { bucket: 'bkt', key: 'pre/t/x.csv', uri: 's3://bkt/pre/t/x.csv' };
			const columnDefs = [{ name: 'id', type: 'BIGINT' }, { name: 'name', type: 'STRING' }];
			client.streamToTableFromS3('t', { columnDefs, s3Path });
			expect(csvStub.createWriteStream.calledOnce).to.be.true;
			const csvOpts = csvStub.createWriteStream.firstCall.args[0];
			expect(csvOpts.headers).to.be.false;
			expect(csvOpts.delimiter).to.equal('|');
			expect(lsStreamsStub.pipeline.calledOnce).to.be.true;
			expect(lsStreamsStub.toS3.calledWith('bkt', 'pre/t/x.csv')).to.be.true;
		});

		it('transform strips Z offset from TIMESTAMP_NTZ columns and applies nonNull', () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'bkt', s3Prefix: 'pre' });
			const columnDefs = [
				{ name: 'created_at', type: 'TIMESTAMP_NTZ' },
				{ name: 'label', type: 'STRING' },
				{ name: 'count', type: 'BIGINT' },
			];
			client.streamToTableFromS3('t', { columnDefs, s3Path: { bucket: 'bkt', key: 'k', uri: 's3://bkt/k' } });
			const transformFn = csvStub.createWriteStream.firstCall.args[0].transform;
			const done = sinon.stub();
			transformFn({ created_at: '2026-01-01T00:00:00Z', label: null, count: 5 }, done);
			expect(done.firstCall.args[0]).to.be.null;
			expect(done.firstCall.args[1]).to.deep.equal(['2026-01-01T00:00:00', '\\N', 5]);
		});

		it('throws when neither s3Path nor bucket/prefix is available', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			const columnDefs = [{ name: 'id', type: 'BIGINT' }];
			expect(() => client.streamToTableFromS3('t', { columnDefs })).to.throw(/s3Bucket\/s3Prefix unresolved/);
		});

		it('uses opts.s3Bucket/s3Prefix when s3Path not provided', () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.s3Bucket = 'bkt';
			client.s3Prefix = 'pre';
			client.auditdate = "'2026-01-01T00:00:00'";
			const columnDefs = [{ name: 'id', type: 'BIGINT' }];
			client.streamToTableFromS3('t', { columnDefs });
			expect(lsStreamsStub.toS3.firstCall.args[0]).to.equal('bkt');
			expect(lsStreamsStub.toS3.firstCall.args[1]).to.match(/^pre\/t\//);
		});

		it('transform excludes _rescued_data from column output', () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'bkt', s3Prefix: 'pre' });
			const columnDefs = [{ name: 'id', type: 'BIGINT' }, { name: '_rescued_data', type: 'STRING' }];
			client.streamToTableFromS3('t', { columnDefs, s3Path: { bucket: 'bkt', key: 'k', uri: 's3://bkt/k' } });
			const transformFn = csvStub.createWriteStream.firstCall.args[0].transform;
			const done = sinon.stub();
			transformFn({ id: 42, _rescued_data: '{"x":1}' }, done);
			expect(done.firstCall.args[1]).to.deep.equal([42]);
		});
	});

	describe('ensureStagingLocation', () => {
		// Three resolution paths to verify:
		//  1. config provides s3Bucket+s3Prefix      → pin them on the client
		//  2. client already pinned                   → idempotent, no query
		//  3. neither set → DESCRIBE SCHEMA EXTENDED  → parse RootLocation row
		// Plus the two error shapes from the UC fallback.

		it('pins s3Bucket/s3Prefix from config when both are provided', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'b1', s3Prefix: 'p1' });
			const queryStub = sinon.stub();
			client.query = queryStub;
			const out = await client.ensureStagingLocation();
			expect(out.s3Bucket).to.equal('b1');
			expect(out.s3Prefix).to.equal('p1');
			expect(client.s3Bucket).to.equal('b1');
			expect(client.s3Prefix).to.equal('p1');
			expect(queryStub.called, 'should not query when config is explicit').to.be.false;
		});

		it('strips a trailing slash from config s3Prefix', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'b1', s3Prefix: 'some/path/' });
			client.query = sinon.stub();
			const out = await client.ensureStagingLocation();
			expect(out.s3Prefix).to.equal('some/path');
		});

		it('is idempotent — second call returns the pinned values without re-querying', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's', s3Bucket: 'b1', s3Prefix: 'p1' });
			const queryStub = sinon.stub();
			client.query = queryStub;
			await client.ensureStagingLocation();
			await client.ensureStagingLocation();
			expect(queryStub.called, 'pinned client should never query').to.be.false;
		});

		it('falls back to DESCRIBE SCHEMA EXTENDED and parses RootLocation when config has no explicit bucket', async () => {
			const client = connectFactory({ catalog: 'mycat', schema: 'mysch' });
			const queryStub = sinon.stub().callsFake((sql, params, cb) => {
				cb(null, [
					{ database_description_item: 'Catalog Name',  database_description_value: 'mycat' },
					{ database_description_item: 'Namespace Name', database_description_value: 'mysch' },
					{ database_description_item: 'RootLocation',  database_description_value: 's3://uc-bucket/managed/mycat/mysch/' },
				]);
			});
			client.query = queryStub;

			const out = await client.ensureStagingLocation();

			expect(queryStub.calledOnce).to.be.true;
			expect(queryStub.firstCall.args[0]).to.equal('DESCRIBE SCHEMA EXTENDED `mycat`.`mysch`');
			expect(out.s3Bucket).to.equal('uc-bucket');
			expect(out.s3Prefix).to.equal('managed/mycat/mysch'); // trailing slash stripped
			expect(client.s3Bucket).to.equal('uc-bucket');
			expect(client.s3Prefix).to.equal('managed/mycat/mysch');
		});

		it('rejects with a clear error when no RootLocation row is returned', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				cb(null, [
					{ database_description_item: 'Catalog Name',   database_description_value: 'c' },
					{ database_description_item: 'Namespace Name', database_description_value: 's' },
					// no RootLocation — schema has no managed location
				]);
			});
			let caught;
			try { await client.ensureStagingLocation(); } catch (e) { caught = e; }
			expect(caught, 'should reject').to.exist;
			expect(caught.message).to.include('Staging location unresolved');
			expect(caught.message).to.include('c.s');
		});

		it('rejects when RootLocation is not an s3:// URL', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				cb(null, [
					{ database_description_item: 'RootLocation', database_description_value: 'dbfs:/mnt/managed/c/s' },
				]);
			});
			let caught;
			try { await client.ensureStagingLocation(); } catch (e) { caught = e; }
			expect(caught).to.exist;
			expect(caught.message).to.include('Unexpected RootLocation format');
		});

		it('propagates the underlying query error from DESCRIBE SCHEMA', async () => {
			const client = connectFactory({ catalog: 'c', schema: 's' });
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				cb(new Error('permission denied'));
			});
			let caught;
			try { await client.ensureStagingLocation(); } catch (e) { caught = e; }
			expect(caught).to.exist;
			expect(caught.message).to.equal('permission denied');
		});
	});
});

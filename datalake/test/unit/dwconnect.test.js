'use strict';

const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

function makeClientStub(schemaByTable) {
	const queryHistory = [];
	const client = {
		auditdate: "'2024-01-01T00:00:00'",
		setAuditdate: sinon.stub(),
		clearSchemaCache: sinon.stub(),
		getSchemaCache: sinon.stub().returns({}),
		setSchemaCache: sinon.stub(),
		escapeId: name => '`' + String(name).toLowerCase() + '`',
		escapeValueNoToLower: v => typeof v === 'string' ? `'${v}'` : v,
		dropTempTables: sinon.stub().resolves(true),
		query: sinon.stub().callsFake((sql, params, cb) => {
			queryHistory.push(sql);
			if (typeof cb === 'function') cb(null, []);
			else if (typeof params === 'function') params(null, []);
		}),
		describeTables: sinon.stub().resolves({}),
		describeTable: sinon.stub().callsFake((table) => {
			if (schemaByTable && schemaByTable[table]) {
				return Promise.resolve(schemaByTable[table]);
			}
			return Promise.reject('NO_SCHEMA_FOUND');
		}),
		streamToTableFromS3: sinon.stub().returns({ pipe: sinon.stub() }),
		_lastStagingView: null,
		_queryHistory: queryHistory,
	};
	return client;
}

describe('dwconnect.js', () => {
	beforeEach(() => {
		proxyquire('../../lib/dwconnect.js', {
			'./connect.js': sinon.stub().returns(makeClientStub()),
			'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
			'leo-streams': {
				through: sinon.stub().returns({ pipe: sinon.stub() }),
				pipe: sinon.stub(),
			},
			'./sql.js': require('../../lib/sql.js'),
			'./surrogate_key.js': require('../../lib/surrogate_key.js'),
		});
	});

	describe('changeTableStructure — empty schema → CREATE TABLE', () => {
		it('issues CREATE TABLE when table does not exist', async () => {
			const connectStub = sinon.stub().returns(makeClientStub({}));
			const factory = proxyquire('../../lib/dwconnect.js', {
				'./connect.js': connectStub,
				'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
				'leo-streams': { through: sinon.stub(), pipe: sinon.stub() },
				'./sql.js': require('../../lib/sql.js'),
				'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			});

			const client = factory({ catalog: 'cat', schema: 'sch' });
			// describeTables resolves with empty cache
			client.describeTables = sinon.stub().resolves({});
			// describeTable rejects NO_SCHEMA_FOUND → triggers createTable
			client.describeTable = sinon.stub().callsFake(() => Promise.reject('NO_SCHEMA_FOUND'));

			const capturedQueries = [];
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				capturedQueries.push(sql);
				if (typeof cb === 'function') cb(null, []);
				else if (typeof params === 'function') params(null, []);
			});
			client.clearSchemaCache = sinon.stub();

			const dOrderDef = {
				isDimension: true,
				structure: {
					'_id': 'sk',
					'id': { nk: true, type: 'integer' },
					'channel': { type: 'varchar(300)' },
				},
			};

			const result = await client.changeTableStructure({ d_order: dOrderDef });

			expect(result.d_order).to.equal('Added');
			const createQuery = capturedQueries.find(q => q.includes('CREATE TABLE'));
			expect(createQuery).to.exist;
			expect(createQuery).to.include('cat.sch.d_order');
		});
	});

	describe('changeTableStructure — schema has missing column → ADD COLUMN', () => {
		it('emits ALTER TABLE ADD COLUMN for new field', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
			];

			const connectStub = sinon.stub().returns(makeClientStub({ d_order: existingSchema }));
			const factory = proxyquire('../../lib/dwconnect.js', {
				'./connect.js': connectStub,
				'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
				'leo-streams': { through: sinon.stub(), pipe: sinon.stub() },
				'./sql.js': require('../../lib/sql.js'),
				'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			});

			const client = factory({ catalog: 'cat', schema: 'sch' });
			client.describeTables = sinon.stub().resolves({});
			client.describeTable = sinon.stub().resolves(existingSchema);

			const capturedQueries = [];
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				capturedQueries.push(sql);
				if (typeof cb === 'function') cb(null, []);
				else if (typeof params === 'function') params(null, []);
			});

			const def = {
				isDimension: false,
				structure: {
					'id': { nk: true, type: 'integer' },
					'channel': { type: 'varchar(300)' }, // new column not in existingSchema
				},
			};

			const result = await client.changeTableStructure({ d_order: def });

			expect(result.d_order).to.equal('Modified');
			const addQueries = capturedQueries.filter(q => q.includes('ADD COLUMN'));
			expect(addQueries.length).to.be.at.least(1);
			const channelQuery = addQueries.find(q => q.includes('channel'));
			expect(channelQuery, 'expected an ADD COLUMN for channel').to.exist;
		});
	});

	describe('dateSk / timeSk — surrogate date/time key computation', () => {
		const { dateSk, timeSk } = require('../../lib/dwconnect.js');

		it('dateSk: reference date 1400-01-01 → 10000', () => {
			expect(dateSk('1400-01-01T00:00:00')).to.equal(10000);
		});

		it('dateSk: one day after reference → 10001', () => {
			expect(dateSk('1400-01-02T00:00:00')).to.equal(10001);
		});

		it('dateSk: accepts space-separated wall-clock format', () => {
			expect(dateSk('1400-01-01 00:00:00')).to.equal(10000);
		});

		it('dateSk: null → 1 (coalesce fallback)', () => {
			expect(dateSk(null)).to.equal(1);
		});

		it('dateSk: undefined → 1', () => {
			expect(dateSk(undefined)).to.equal(1);
		});

		it('timeSk: midnight → 10000', () => {
			expect(timeSk('2024-01-15T00:00:00')).to.equal(10000);
		});

		it('timeSk: 14:30:45 → 62245', () => {
			// 14*3600 + 30*60 + 45 + 10000 = 50400 + 1800 + 45 + 10000 = 62245
			expect(timeSk('2024-01-15T14:30:45')).to.equal(62245);
		});

		it('timeSk: accepts space-separated wall-clock format', () => {
			expect(timeSk('2024-01-15 14:30:45')).to.equal(62245);
		});

		it('timeSk: null → 1', () => {
			expect(timeSk(null)).to.equal(1);
		});
	});

	describe('changeTableStructure — FK columns for dimension links', () => {
		function makeFactoryWithSchema(existingSchema) {
			const connectStub = sinon.stub().returns(makeClientStub({ f_order_item: existingSchema }));
			const factory = proxyquire('../../lib/dwconnect.js', {
				'./connect.js': connectStub,
				'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
				'leo-streams': { through: sinon.stub(), pipe: sinon.stub() },
				'./sql.js': require('../../lib/sql.js'),
				'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			});
			const client = factory({ catalog: 'cat', schema: 'sch' });
			client.describeTables = sinon.stub().resolves({});
			client.describeTable = sinon.stub().resolves(existingSchema);
			const capturedQueries = [];
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				capturedQueries.push(sql);
				if (typeof cb === 'function') cb(null, []);
				else if (typeof params === 'function') params(null, []);
			});
			return { client, capturedQueries };
		}

		it('adds entity FK column when dimension link column is missing', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: 'item_id', data_type: 'INT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
				{ column_name: '_rescued_data', data_type: 'STRING' },
			];
			const { client, capturedQueries } = makeFactoryWithSchema(existingSchema);
			const def = {
				isDimension: false,
				structure: {
					'id': { nk: true, type: 'integer' },
					'item_id': { type: 'integer', dimension: 'd_item' },
				},
			};
			await client.changeTableStructure({ f_order_item: def });
			const addCols = capturedQueries.filter(q => q.includes('ADD COLUMN'));
			expect(addCols.some(q => q.includes('d_item'))).to.be.true;
		});

		it('adds _date and _time INT columns for datetime dimension link', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: 'occurred_at', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
				{ column_name: '_rescued_data', data_type: 'STRING' },
			];
			const { client, capturedQueries } = makeFactoryWithSchema(existingSchema);
			const def = {
				isDimension: false,
				structure: {
					'id': { nk: true, type: 'integer' },
					'occurred_at': { type: 'timestamp', dimension: 'datetime' },
				},
			};
			await client.changeTableStructure({ f_order_item: def });
			const addCols = capturedQueries.filter(q => q.includes('ADD COLUMN'));
			expect(addCols.some(q => q.includes('d_occurred_at_date'))).to.be.true;
			expect(addCols.some(q => q.includes('d_occurred_at_time'))).to.be.true;
		});

		it('skips FK ADD COLUMN when FK column already exists', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: 'item_id', data_type: 'INT' },
				{ column_name: 'd_item', data_type: 'BIGINT' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
				{ column_name: '_rescued_data', data_type: 'STRING' },
			];
			const { client, capturedQueries } = makeFactoryWithSchema(existingSchema);
			const def = {
				isDimension: false,
				structure: {
					'id': { nk: true, type: 'integer' },
					'item_id': { type: 'integer', dimension: 'd_item' },
				},
			};
			await client.changeTableStructure({ f_order_item: def });
			const addCols = capturedQueries.filter(q => q.includes('ADD COLUMN') && q.includes('d_item'));
			expect(addCols).to.have.length(0);
		});
	});

	describe('linkDimensions — no-op', () => {
		it('calls done(null) without error', done => {
			const connectStub = sinon.stub().returns(makeClientStub());
			const factory = proxyquire('../../lib/dwconnect.js', {
				'./connect.js': connectStub,
				'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
				'leo-streams': { through: sinon.stub(), pipe: sinon.stub() },
				'./sql.js': require('../../lib/sql.js'),
				'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			});
			const client = factory({ catalog: 'cat', schema: 'sch' });
			client.linkDimensions('f_order_item', [], ['id'], err => {
				expect(err).to.not.exist;
				done();
			});
		});
	});

	describe('withRetry — bounded idempotent retry', () => {
		const { withRetry } = require('../../lib/dwconnect.js');

		it('calls callback with result on first-try success', (done) => {
			let calls = 0;
			withRetry(cb => { calls++; cb(null, 'ok'); }, {}, (err, result) => {
				expect(err).to.be.null;
				expect(result).to.equal('ok');
				expect(calls).to.equal(1);
				done();
			});
		});

		it('retries on connection-class error and succeeds on second attempt', (done) => {
			let calls = 0;
			const connErr = new Error('ECONNRESET');
			connErr.code = 'ECONNRESET';
			withRetry(cb => {
				calls++;
				if (calls === 1) return cb(connErr);
				cb(null, 'recovered');
			}, { backoffMs: 0 }, (err, result) => {
				expect(err).to.be.null;
				expect(result).to.equal('recovered');
				expect(calls).to.equal(2);
				done();
			});
		});

		it('does NOT retry on query-class SQL error', (done) => {
			let calls = 0;
			const sqlErr = new Error('[PARSE_SYNTAX_ERROR] bad sql');
			withRetry(cb => { calls++; cb(sqlErr); }, { backoffMs: 0 }, (err) => {
				expect(err).to.equal(sqlErr);
				expect(calls).to.equal(1);
				done();
			});
		});

		it('is bounded — fails after maxAttempts on persistent connection errors', (done) => {
			let calls = 0;
			const connErr = new Error('socket hang up');
			withRetry(cb => { calls++; cb(connErr); }, { attempts: 3, backoffMs: 0 }, (err) => {
				expect(err).to.equal(connErr);
				expect(calls).to.equal(3);
				done();
			});
		});

		it('re-acquires a fresh session per retry (each fn() call is independent)', (done) => {
			// Each retry passes a fresh function call — there is no shared session state.
			// Verify by asserting the fn receives independent invocations.
			const sessions = [];
			let callIdx = 0;
			const connErr = new Error('ECONNRESET');
			connErr.code = 'ECONNRESET';
			withRetry(cb => {
				sessions.push(++callIdx);
				if (callIdx < 3) return cb(connErr);
				cb(null, 'done');
			}, { attempts: 3, backoffMs: 0 }, (err, result) => {
				expect(err).to.be.null;
				expect(result).to.equal('done');
				expect(sessions).to.deep.equal([1, 2, 3]);
				done();
			});
		});
	});

	describe('changeTableStructure — schema unchanged → Unmodified', () => {
		it('returns Unmodified when all columns present', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: 'channel', data_type: 'STRING' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
				{ column_name: '_rescued_data', data_type: 'STRING' },
			];

			const connectStub = sinon.stub().returns(makeClientStub({ d_order: existingSchema }));
			const factory = proxyquire('../../lib/dwconnect.js', {
				'./connect.js': connectStub,
				'leo-logger': { info: () => {}, debug: () => {}, error: () => {} },
				'leo-streams': { through: sinon.stub(), pipe: sinon.stub() },
				'./sql.js': require('../../lib/sql.js'),
				'./surrogate_key.js': require('../../lib/surrogate_key.js'),
			});

			const client = factory({ catalog: 'cat', schema: 'sch' });
			client.describeTables = sinon.stub().resolves({});
			client.describeTable = sinon.stub().resolves(existingSchema);

			const capturedQueries = [];
			client.query = sinon.stub().callsFake((sql, params, cb) => {
				capturedQueries.push(sql);
				if (typeof cb === 'function') cb(null, []);
				else if (typeof params === 'function') params(null, []);
			});

			const def = {
				isDimension: false,
				structure: {
					'id': { nk: true, type: 'integer' },
					'channel': { type: 'varchar(300)' },
				},
			};

			const result = await client.changeTableStructure({ d_order: def });

			expect(result.d_order).to.equal('Unmodified');
			expect(capturedQueries.filter(q => q.includes('ALTER') || q.includes('CREATE'))).to.have.length(0);
		});
	});
});

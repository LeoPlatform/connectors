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

	describe('literalForType — type-aware naturalKeyFilter quoting', () => {
		// Mirrors the dim-vs-fact branching in ../postgres/lib/dwconnect.js
		// naturalKeyFilter: numeric column types render as unquoted literals
		// (so >= compares numerically), everything else quoted.
		const { literalForType } = require('../../lib/dwconnect.js');
		const escape = v => typeof v === 'string' ? `'${v.replace(/'/g, "\\'")}'` : v;

		it('numeric types render unquoted', () => {
			expect(literalForType(12345, 'BIGINT', escape)).to.equal('12345');
			expect(literalForType(7, 'INT', escape)).to.equal('7');
			expect(literalForType(42, 'INTEGER', escape)).to.equal('42');
			expect(literalForType('99.5', 'DECIMAL', escape)).to.equal('99.5');
			expect(literalForType(1.5, 'DOUBLE', escape)).to.equal('1.5');
		});

		it('string and timestamp types render quoted', () => {
			expect(literalForType('abc', 'STRING', escape)).to.equal("'abc'");
			expect(literalForType('2026-01-01 00:00:00', 'TIMESTAMP_NTZ', escape))
				.to.equal("'2026-01-01 00:00:00'");
			expect(literalForType('2026-01-01', 'DATE', escape)).to.equal("'2026-01-01'");
		});

		it('case-insensitive on type name', () => {
			expect(literalForType(1, 'bigint', escape)).to.equal('1');
			expect(literalForType('x', 'string', escape)).to.equal("'x'");
		});

		it('unknown type falls back to quoted (safe default)', () => {
			expect(literalForType('x', '', escape)).to.equal("'x'");
			expect(literalForType('x', undefined, escape)).to.equal("'x'");
		});
	});

	describe('changeTableStructure — schema unchanged → Unmodified', () => {
		it('returns Unmodified when all columns present', async () => {
			const existingSchema = [
				{ column_name: 'id', data_type: 'INT' },
				{ column_name: 'channel', data_type: 'STRING' },
				{ column_name: '_auditdate', data_type: 'TIMESTAMP_NTZ' },
				{ column_name: '_deleted', data_type: 'BOOLEAN' },
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

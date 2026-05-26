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
	}, overrides && overrides.client);

	return {
		DBSQLClient: sinon.stub().returns(clientStub),
		_session: sessionStub,
		_client: clientStub,
	};
}

describe('connect.js', () => {
	let connectFactory, databricksStub;

	beforeEach(() => {
		databricksStub = makeDatabricksStub();
		connectFactory = proxyquire('../../lib/connect.js', {
			'@databricks/sql': databricksStub,
			'leo-logger': () => ({ info: () => {}, debug: () => {}, error: () => {} }),
			'leo-streams': {
				pipeline: sinon.stub(),
				write: sinon.stub(),
				toS3: sinon.stub(),
			},
			'fast-csv': { createWriteStream: sinon.stub() },
		});
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
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00Z')).to.equal('2026-03-15T14:30:00');
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00.123Z')).to.equal('2026-03-15T14:30:00.123');
			});

			it('strips ±HH:MM offsets', () => {
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00-08:00')).to.equal('2026-03-15T14:30:00');
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00+05:30')).to.equal('2026-03-15T14:30:00');
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00.250+00:00')).to.equal('2026-03-15T14:30:00.250');
			});

			it('strips ±HHMM offsets (no colon)', () => {
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00-0800')).to.equal('2026-03-15T14:30:00');
			});

			it('accepts space separator between date and time', () => {
				expect(realConnect._stripTimestampOffset('2026-03-15 14:30:00Z')).to.equal('2026-03-15 14:30:00');
			});

			it('leaves naked ISO unchanged', () => {
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00')).to.equal('2026-03-15T14:30:00');
				expect(realConnect._stripTimestampOffset('2026-03-15T14:30:00.123')).to.equal('2026-03-15T14:30:00.123');
			});

			it('leaves non-string and non-matching values unchanged', () => {
				expect(realConnect._stripTimestampOffset(null)).to.equal(null);
				expect(realConnect._stripTimestampOffset(undefined)).to.equal(undefined);
				expect(realConnect._stripTimestampOffset(42)).to.equal(42);
				expect(realConnect._stripTimestampOffset('not a timestamp')).to.equal('not a timestamp');
				// Pure date — different shape, must pass through; DATE columns are
				// not NTZ anyway, but defense in depth.
				expect(realConnect._stripTimestampOffset('2026-03-15')).to.equal('2026-03-15');
			});
		});

		describe('isNtzType', () => {
			it('matches TIMESTAMP_NTZ in any case', () => {
				expect(realConnect._isNtzType('TIMESTAMP_NTZ')).to.equal(true);
				expect(realConnect._isNtzType('timestamp_ntz')).to.equal(true);
				expect(realConnect._isNtzType(' Timestamp_Ntz ')).to.equal(true);
			});

			it('rejects zone-aware TIMESTAMP and other types', () => {
				// TIMESTAMP (zone-aware in Databricks) MUST NOT be stripped — the
				// offset is meaningful for that type.
				expect(realConnect._isNtzType('TIMESTAMP')).to.equal(false);
				expect(realConnect._isNtzType('STRING')).to.equal(false);
				expect(realConnect._isNtzType('DATE')).to.equal(false);
				expect(realConnect._isNtzType(null)).to.equal(false);
				expect(realConnect._isNtzType(undefined)).to.equal(false);
			});
		});
	});

	describe('connect() + query()', () => {
		it('calls DBSQLClient.connect with host/path/token', async () => {
			const client = connectFactory({ host: 'myhost', path: '/sql/1', token: 'mytoken', catalog: 'cat', schema: 'sch' });
			await client.connect();
			expect(databricksStub._client.connect.calledOnce).to.be.true;
			const args = databricksStub._client.connect.firstCall.args[0];
			expect(args.host).to.equal('myhost');
			expect(args.path).to.equal('/sql/1');
			expect(args.token).to.equal('mytoken');
		});

		it('opens session with initialCatalog and initialSchema', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'mycat', schema: 'mysch' });
			await client.connect();
			const sessionArgs = databricksStub._client.openSession.firstCall.args[0];
			expect(sessionArgs.initialCatalog).to.equal('mycat');
			expect(sessionArgs.initialSchema).to.equal('mysch');
		});

		it('passes ansi_mode=false, infer_timestamp_ntz_type=true, timezone=UTC in session params', async () => {
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			await client.connect();
			const params = databricksStub._client.openSession.firstCall.args[0].initialParameters;
			expect(params.ansi_mode).to.equal('false');
			expect(params.infer_timestamp_ntz_type).to.equal('true');
			expect(params.timezone).to.equal('UTC');
		});

		it('propagates query errors via callback', (done) => {
			databricksStub._session.executeStatement = sinon.stub().rejects(new Error('exec failed'));
			const client = connectFactory({ host: 'h', path: '/p', token: 't', catalog: 'c', schema: 's' });
			client.query('SELECT 1', [], (err) => {
				expect(err).to.be.instanceOf(Error);
				expect(err.message).to.equal('exec failed');
				done();
			});
		});
	});
});

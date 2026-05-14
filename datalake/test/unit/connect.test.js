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

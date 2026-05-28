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
			it('builds {bucket, key, uri} deterministically from inputs', () => {
				const p = realConnect.stagingS3Path('bkt', 'prefix/sub', 'f_order', "'2026-03-15T14:30:00'");
				expect(p.bucket).to.equal('bkt');
				expect(p.key).to.equal('prefix/sub/f_order/2026-03-15T14-30-00.csv');
				expect(p.uri).to.equal('s3://bkt/prefix/sub/f_order/2026-03-15T14-30-00.csv');
			});

			it('strips a trailing slash from the prefix', () => {
				const p = realConnect.stagingS3Path('bkt', 'prefix/sub/', 't', "'2026-01-01T00:00:00'");
				expect(p.key).to.equal('prefix/sub/t/2026-01-01T00-00-00.csv');
			});

			it('strips surrounding single quotes and replaces colons in the auditdate', () => {
				const p = realConnect.stagingS3Path('b', 'p', 't', "'2026-03-15T14:30:00.123Z'");
				// colons → dashes; quotes stripped; rest preserved
				expect(p.key).to.equal('p/t/2026-03-15T14-30-00.123Z.csv');
			});

			it('two parallel callers with different tables produce distinct paths', () => {
				const auditdate = "'2026-03-15T14:30:00'";
				const a = realConnect.stagingS3Path('b', 'p', 'f_order', auditdate);
				const b = realConnect.stagingS3Path('b', 'p', 'f_order_item', auditdate);
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

'use strict';
const { expect } = require('chai');
const sinon = require('sinon');
const { PassThrough } = require('stream');
const dwconnect = require('../../lib/dwconnect.js');

describe('dwconnect', function() {
	let client;
	let sandbox;

	const factColumns = [
		{ column_name: 'id' },
		{ column_name: 'amount' },
		{ column_name: '_deleted' },
		{ column_name: '_auditdate' },
	];

	beforeEach(function() {
		sandbox = sinon.createSandbox();
		// The pool is lazy: nothing connects unless a query is issued, and every test asserts none is.
		client = dwconnect({});
		sandbox.stub(client, 'query').callsFake(() => {
			throw new Error('unexpected query');
		});
	});

	afterEach(async function() {
		sandbox.restore();
		await client.end();
	});

	describe('importFact', function() {
		it('throws a named error when the table is missing from the schema cache', function() {
			// The offload bot populates the cache, alters a table, then clears it before loading.
			client.setSchemaCache({ 'public.f_order': factColumns });
			client.clearSchemaCache();
			const callback = sinon.spy();

			expect(() => client.importFact(new PassThrough({ objectMode: true }), 'f_order', 'id', callback))
				.to.throw(Error, 'public.f_order not found in schema');
			expect(callback.called).to.equal(false);
			expect(client.query.called).to.equal(false);
		});

		it('does not add an undefined staging entry to the schema cache when the table is missing', function() {
			client.clearSchemaCache();

			expect(() => client.importFact(new PassThrough({ objectMode: true }), 'f_order', 'id', () => {})).to.throw();
			expect(client.getSchemaCache()).to.deep.equal({});
		});

		it('registers the staging table from the cached schema when the table is present', function(done) {
			client.setSchemaCache({ 'public.f_order': factColumns });
			const stop = new Error('stop before touching the database');
			sandbox.stub(client, 'describeTable').rejects(stop);

			client.importFact(new PassThrough({ objectMode: true }), 'f_order', 'id', (err) => {
				expect(err).to.equal(stop);
				expect(client.getSchemaCache()['public.staging_f_order']).to.equal(factColumns);
				expect(client.query.called).to.equal(false);
				done();
			});
		});
	});

	describe('importDimension', function() {
		it('throws the same named error as importFact when the table is missing from the schema cache', function() {
			client.clearSchemaCache();

			expect(() => client.importDimension(new PassThrough({ objectMode: true }), 'd_account', 'd_id', ['id'], {}, () => {}))
				.to.throw(Error, 'public.d_account not found in schema');
		});
	});
});

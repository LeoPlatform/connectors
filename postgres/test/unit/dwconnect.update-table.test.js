'use strict';
const { expect } = require('chai');
const sinon = require('sinon');
const dwconnect = require('../../lib/dwconnect.js');

describe('dwconnect table changes', function() {
	let client;
	let sandbox;
	let queries;

	// A datetime dimension field: on Redshift, updateTable adds it as three separate ALTERs.
	const requestedShipAt = { type: 'timestamp', dimension: 'd_datetime' };
	// public.d_order on 2026-08-27 after a run stopped part-way: the first ALTER had committed, the other two had not.
	const partlyAltered = [
		{ column_name: 'id' },
		{ column_name: 'd_requested_ship_at_date' },
	];
	const dOrder = { d_order: { isDimension: true, structure: { id: 'sk', requested_ship_at: requestedShipAt } } };

	beforeEach(function() {
		sandbox = sinon.createSandbox();
		// general's offload-redshift config. The pool is lazy: nothing connects, because every test stubs query.
		client = dwconnect({ version: 'redshift', hashedSurrogateKeys: true, bypassSlowlyChangingDimensions: true });
		queries = [];
	});

	afterEach(async function() {
		sandbox.restore();
		await client.end();
	});

	// Records each query and answers it asynchronously, like pg, with the error errorFor(sql) returns (or none).
	function stubQuery(errorFor = () => null) {
		sandbox.stub(client, 'query').callsFake((sql, callback) => {
			queries.push(sql.replace(/\s+/g, ' ').trim());
			setImmediate(() => callback(errorFor(sql)));
		});
	}

	const duplicateColumn = column => Object.assign(
		new Error(`column "${column}" of relation "d_order" already exists`),
		{ code: '42701' }
	);

	describe('updateTable', function() {
		it('adds every column when none exist yet', async function() {
			stubQuery();

			await client.updateTable('d_order', { requested_ship_at: requestedShipAt });

			expect(queries).to.deep.equal([
				'alter table d_order add column d_requested_ship_at_date integer',
				'alter table d_order add column d_requested_ship_at_time integer',
				'alter table d_order add column "requested_ship_at" timestamp',
			]);
		});

		it('skips columns the table already has', async function() {
			stubQuery();

			await client.updateTable('d_order', { requested_ship_at: requestedShipAt }, { d_requested_ship_at_date: {} });

			expect(queries).to.deep.equal([
				'alter table d_order add column d_requested_ship_at_time integer',
				'alter table d_order add column "requested_ship_at" timestamp',
			]);
		});

		it('issues no ALTER when every column already exists', async function() {
			stubQuery();

			await client.updateTable('d_order', { requested_ship_at: requestedShipAt }, {
				d_requested_ship_at_date: {},
				d_requested_ship_at_time: {},
				requested_ship_at: {},
			});

			expect(queries).to.deep.equal([]);
		});

		it('carries on past a column that another run added in the meantime', async function() {
			stubQuery(sql => sql.includes('d_requested_ship_at_date') ? duplicateColumn('d_requested_ship_at_date') : null);

			await client.updateTable('d_order', { requested_ship_at: requestedShipAt });

			expect(queries).to.have.length(3);
		});

		it('rejects with any other error, and stops issuing ALTERs', async function() {
			const denied = new Error('permission denied for relation d_order');
			stubQuery(() => denied);

			const err = await client.updateTable('d_order', { requested_ship_at: requestedShipAt }).then(() => null, e => e);

			expect(err).to.equal(denied);
			expect(queries).to.have.length(1);
		});
	});

	describe('changeTableStructure', function() {
		beforeEach(function() {
			sandbox.stub(client, 'describeTables').resolves({});
			sandbox.stub(client, 'describeTable').resolves(partlyAltered);
		});

		it('finishes a column add that an earlier run only partly applied', async function() {
			stubQuery();

			const results = await client.changeTableStructure(dOrder);

			expect(results).to.deep.equal({ d_order: 'Modified' });
			expect(queries).to.deep.equal([
				'alter table d_order add column d_requested_ship_at_time integer',
				'alter table d_order add column "requested_ship_at" timestamp',
			]);
		});

		it('rejects when an ALTER fails, instead of throwing out of a callback', async function() {
			const denied = new Error('permission denied for relation d_order');
			stubQuery(() => denied);

			const err = await client.changeTableStructure(dOrder).then(() => null, e => e);

			expect(err).to.equal(denied);
		});
	});
});

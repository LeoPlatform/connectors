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

		it('types a dimension link column as bigint when surrogate keys are hashed', async function() {
			stubQuery();

			await client.updateTable('f_order', { product_id: { type: 'integer', dimension: 'd_product' } });

			expect(queries).to.deep.equal([
				'alter table f_order add column d_product bigint',
				'alter table f_order add column "product_id" integer',
			]);
		});

		it('adds a dim_time field as a surrogate time key', async function() {
			stubQuery();

			await client.updateTable('f_order', { ship_time: { type: 'time', dimension: 'dim_time' } });

			expect(queries).to.deep.equal([
				'alter table f_order add column d_ship_time_time integer',
				'alter table f_order add column "ship_time" time',
			]);
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
			// Redshift refuses to add the column the earlier run already committed.
			stubQuery(sql => sql.includes('add column d_requested_ship_at_date ') ? duplicateColumn('d_requested_ship_at_date') : null);

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

		it('adds nothing once a datetime dimension has its _date, _time and source columns', async function() {
			// On 2026-08-27 the run after the columns landed added an unused "d_requested_ship_at" bigint.
			client.describeTable.resolves(partlyAltered.concat([
				{ column_name: 'd_requested_ship_at_time' },
				{ column_name: 'requested_ship_at' },
			]));
			stubQuery();

			const results = await client.changeTableStructure(dOrder);

			expect(results).to.deep.equal({ d_order: 'Unmodified' });
			expect(queries).to.deep.equal([]);
		});

		it('rejects when creating a missing table fails, instead of throwing out of a callback', async function() {
			client.describeTable.callsFake(() => Promise.reject('NO_SCHEMA_FOUND'));
			const denied = new Error('permission denied for schema public');
			stubQuery(() => denied);

			const err = await client.changeTableStructure(dOrder).then(() => null, e => e);

			expect(err).to.equal(denied);
			expect(queries).to.have.length(1);
		});
	});

	describe('reading the table list', function() {
		// 23 prod runs failed on this first query between 2026-10-09 and 2026-10-23 (ECONNREFUSED / ETIMEDOUT).
		const refused = new Error('connect ECONNREFUSED 10.0.0.1:5439');

		it('changeTableStructure rejects when the schema query fails, instead of throwing out of a callback', async function() {
			stubQuery(() => refused);

			const err = await client.changeTableStructure(dOrder).then(() => null, e => e);

			expect(err).to.equal(refused);
		});

		it('describeTable rejects when the schema query fails', async function() {
			stubQuery(() => refused);

			const err = await client.describeTable('d_order').then(() => null, e => e);

			expect(err).to.equal(refused);
		});
	});
});

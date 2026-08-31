const { assert } = require('chai');

// combine.js requires leo-sdk at module load; provide a config so the require cannot throw in a
// bare test environment. combine only uses leo.streams (pure stream helpers) — nothing hits AWS.
process.env.RSTREAMS_CONFIG = JSON.stringify({
	Region: 'us-east-1',
	LeoStream: 'test-LeoStream',
	LeoCron: 'test-LeoCron',
	LeoEvent: 'test-LeoEvent',
	LeoS3: 'test-leos3',
	LeoKinesisStream: 'test-LeoKinesisStream',
	LeoFirehoseStream: 'test-LeoFirehoseStream',
	LeoSettings: 'test-LeoSettings'
});

const combine = require('../../datawarehouse/combine');

const TABLE = 'f_current_inventory_state';
const NKS = { [TABLE]: ['item_id', 'supplier_id', 'retailer_id'] };
const ORDERED = { orderFields: { [TABLE]: 'source_eid' } };

// Realistic fixed-width RStreams eids: EID_1 < EID_2 < EID_3.
const EID_1 = 'z/2026/08/26/12/00/1787000000000-0000001';
const EID_2 = 'z/2026/08/26/12/00/1787000000000-0000002';
const EID_3 = 'z/2026/08/26/12/01/1787000060000-0000001';

function evt(data) {
	return { eid: 'queue-eid-unused', payload: { entity: 'current_inventory_state', type: 'fact', data } };
}

function row(overrides) {
	return Object.assign({ item_id: 1, supplier_id: 2, retailer_id: 3 }, overrides);
}

// Pipe events through combine() and collect the folded rows for TABLE.
function fold(events, opts) {
	return new Promise((resolve, reject) => {
		const c = combine(NKS, opts);
		c.on('error', reject);
		c.on('data', (tables) => {
			const t = tables[TABLE];
			if (!t) {
				return resolve([]);
			}
			const rows = [];
			t.stream.on('data', (r) => rows.push(r));
			t.stream.on('error', reject);
			t.stream.on('end', () => resolve(rows));
		});
		events.forEach((e) => c.write(e));
		c.end();
	});
}

describe('datawarehouse combine fold', () => {

	describe('default mode (no orderFields)', () => {
		it('keeps the LAST row per key by arrival — the pre-existing behaviour, locked', async () => {
			const rows = await fold([
				evt(row({ quantity: 5, status: 'out-of-stock', source_eid: EID_2 })), // fresher, arrives first
				evt(row({ quantity: 9, status: 'in-stock', source_eid: EID_1 })), // stale, arrives last
			]);
			assert.lengthOf(rows, 1);
			assert.equal(rows[0].quantity, 9, 'arrival order wins when no order field is configured');
			assert.equal(rows[0].status, 'in-stock');
		});

		it('is unaffected by an orderFields entry for a different table', async () => {
			const rows = await fold(
				[
					evt(row({ quantity: 5, source_eid: EID_2 })),
					evt(row({ quantity: 9, source_eid: EID_1 })),
				],
				{ orderFields: { some_other_table: 'source_eid' } },
			);
			assert.lengthOf(rows, 1);
			assert.equal(rows[0].quantity, 9, 'the opt-in is strictly per table');
		});
	});

	describe('ordered mode (orderFields: { table: source_eid })', () => {
		it('repairs an out-of-order arrival: the highest source_eid wins the fold', async () => {
			const rows = await fold(
				[
					evt(row({ quantity: 5, status: 'out-of-stock', source_eid: EID_2 })), // fresher, arrives FIRST
					evt(row({ quantity: 9, status: 'in-stock', source_eid: EID_1 })), // stale, arrives LAST
				],
				ORDERED,
			);
			assert.lengthOf(rows, 1);
			assert.equal(rows[0].quantity, 5, 'the stale late arrival must not win');
			assert.equal(rows[0].status, 'out-of-stock');
			assert.equal(rows[0].source_eid, EID_2);
		});

		it('a row with no source_eid sorts before every row that has one (backfill loses to live)', async () => {
			// The design-required assertion: backfill rows carry no source_eid and must lose the
			// fold against live rows, regardless of arrival order.
			const liveFirst = await fold(
				[
					evt(row({ quantity: 5, status: 'out-of-stock', source_eid: EID_1 })), // live
					evt(row({ quantity: 77, status: 'in-stock', source_eid: null })), // backfill, arrives last
				],
				ORDERED,
			);
			assert.lengthOf(liveFirst, 1);
			assert.equal(liveFirst[0].quantity, 5, 'a live row must beat a backfill row that arrives after it');

			const backfillFirst = await fold(
				[
					evt(row({ quantity: 77, status: 'in-stock', source_eid: null })), // backfill first
					evt(row({ quantity: 5, status: 'out-of-stock', source_eid: EID_1 })), // live
				],
				ORDERED,
			);
			assert.lengthOf(backfillFirst, 1);
			assert.equal(backfillFirst[0].quantity, 5, 'a live row must beat a backfill row in either arrival order');
		});

		it('a backfill-only key still passes through', async () => {
			const rows = await fold([evt(row({ quantity: 77, status: 'in-stock', source_eid: null }))], ORDERED);
			assert.lengthOf(rows, 1);
			assert.equal(rows[0].quantity, 77);
		});

		it('an explicit null on the fresher row survives the fold (null is a value, not a gap)', async () => {
			// Complete-snapshot semantics: NULL means "unknown" and must be storable — the fresher
			// row's null has to overwrite the stale row's value, not be back-filled by it.
			const rows = await fold(
				[
					evt(row({ quantity: null, status: null, source_eid: EID_2 })), // fresher, null snapshot
					evt(row({ quantity: 9, status: 'in-stock', source_eid: EID_1 })), // stale
				],
				ORDERED,
			);
			assert.lengthOf(rows, 1);
			assert.isNull(rows[0].quantity, 'the fresher explicit null must win');
			assert.isNull(rows[0].status);
		});

		it('equal source_eids tie-break by arrival order', async () => {
			// Real case: qty-zero and availability-changed from the same source event share one
			// source_eid and one natural key; the later arrival wins field-wise, deterministically.
			const rows = await fold(
				[
					evt(row({ quantity: 0, status: 'out-of-stock', source_eid: EID_3 })),
					evt(row({ quantity: 0, status: 'discontinued', source_eid: EID_3 })),
				],
				ORDERED,
			);
			assert.lengthOf(rows, 1);
			assert.equal(rows[0].status, 'discontinued');
		});

		it('folds each natural key independently', async () => {
			const rows = await fold(
				[
					evt(row({ item_id: 1, quantity: 5, source_eid: EID_2 })),
					evt(row({ item_id: 1, quantity: 9, source_eid: EID_1 })), // stale for key 1
					evt(row({ item_id: 42, quantity: 100, source_eid: EID_1 })),
				],
				ORDERED,
			);
			assert.lengthOf(rows, 2);
			const byItem = {};
			rows.forEach((r) => (byItem[r.item_id] = r));
			assert.equal(byItem[1].quantity, 5);
			assert.equal(byItem[42].quantity, 100);
		});
	});
});

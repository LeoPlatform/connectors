'use strict';

// Synthetic fact-table fixture used by round_trip / idempotency / schema_evolution.
// Mixed-type structure exercises the type-mapping table from build_plan.md Step 5:
// bigint NK, varchar→STRING, timestamp→TIMESTAMP_NTZ, boolean, decimal(p,s), int.

const TABLE = 'f_datalake_connector_test';

const tableDef = {
	structure: {
		id: 'bigint',
		sk: 'sk',
		name: 'varchar(255)',
		status: 'varchar(50)',
		created_at: 'timestamp',
		is_active: 'boolean',
		score: 'decimal(10,2)',
		quantity: 'int',
	},
	isDimension: false,
};

// Deterministic record generator. Counter-based — no PRNG so results are reproducible.
function makeRecords(n) {
	const out = [];
	for (let i = 1; i <= n; i++) {
		out.push({
			id: i,
			name: `record_${i}`,
			status: i % 2 === 0 ? 'active' : 'pending',
			created_at: `2026-01-${String((i % 28) + 1).padStart(2, '0')}T12:34:56`,
			is_active: i % 3 === 0,
			score: (i * 1.5).toFixed(2),
			quantity: i * 10,
		});
	}
	return out;
}

// dw_fields tableDef variant with an extra column (for schema_evolution test).
function tableDefWithExtraColumn() {
	return Object.assign({}, tableDef, {
		structure: Object.assign({}, tableDef.structure, {
			extra_col: 'varchar(50)',
		}),
	});
}

module.exports = { TABLE, tableDef, makeRecords, tableDefWithExtraColumn };

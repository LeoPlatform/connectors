'use strict';

const { expect } = require('chai');
const { mapType, createTable, alterAddColumn, alterColumnType, mergeFact } = require('../../lib/sql.js');

const escapeId = name => '`' + String(name).toLowerCase().replace(/`/g, '') + '`';

const columnConfig = {
	_auditdate: '_auditdate',
	_deleted: '_deleted',
};

describe('sql.js', () => {

	describe('mapType', () => {
		const cases = [
			['varchar(300)', 'STRING'],
			['varchar(1)', 'STRING'],
			['VARCHAR(100)', 'STRING'],
			['timestamp', 'TIMESTAMP_NTZ'],
			['TIMESTAMP', 'TIMESTAMP_NTZ'],
			['timestamptz', 'TIMESTAMP'],
			['date', 'DATE'],
			['boolean', 'BOOLEAN'],
			['BOOLEAN', 'BOOLEAN'],
			['int', 'INT'],
			['integer', 'INT'],
			['bigint', 'BIGINT'],
			['float', 'FLOAT'],
			['decimal', 'DECIMAL(18,0)'],
			['DECIMAL', 'DECIMAL(18,0)'],
			['decimal(10,2)', 'DECIMAL(10,2)'],
			['DECIMAL(38,10)', 'DECIMAL(38,10)'],
		];

		cases.forEach(([input, expected]) => {
			it(`maps ${input} → ${expected}`, () => {
				expect(mapType(input)).to.equal(expected);
			});
		});

		it('returns STRING for unknown types', () => {
			expect(mapType('unknowntype')).to.equal('STRING');
		});
	});

	describe('createTable', () => {
		const dOrderDef = {
			isDimension: true,
			structure: {
				'_id': 'sk',
				'id': { nk: true, type: 'integer' },
				'channel': { type: 'varchar(300)' },
				'archived': { type: 'boolean' },
				'created_at': { type: 'timestamp' },
				'cost': { type: 'integer' },
			},
		};

		it('emits CREATE TABLE IF NOT EXISTS ... USING DELTA', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('CREATE TABLE IF NOT EXISTS cat.sch.d_order');
			expect(ddl).to.include('USING DELTA');
		});

		it('maps sk field to BIGINT', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('`_id` BIGINT');
		});

		it('maps varchar(n) → STRING', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('`channel` STRING');
		});

		it('maps timestamp → TIMESTAMP_NTZ', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('`created_at` TIMESTAMP_NTZ');
		});

		it('maps boolean → BOOLEAN', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('`archived` BOOLEAN');
		});

		it('appends _auditdate TIMESTAMP_NTZ and _deleted BOOLEAN', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.include('`_auditdate` TIMESTAMP_NTZ');
			expect(ddl).to.include('`_deleted` BOOLEAN');
		});

		it('appends CLUSTER BY when clusterKey set', () => {
			const def = Object.assign({}, dOrderDef, { clusterKey: 'id' });
			const ddl = createTable('cat.sch.f_order_item', def, columnConfig, escapeId);
			expect(ddl).to.include('CLUSTER BY (`id`)');
		});

		it('omits CLUSTER BY when clusterKey absent', () => {
			const ddl = createTable('cat.sch.d_order', dOrderDef, columnConfig, escapeId);
			expect(ddl).to.not.include('CLUSTER BY');
		});

		it('lowercases all identifiers via escapeId', () => {
			const defMixed = {
				isDimension: false,
				structure: { 'OrderId': { type: 'integer', nk: true } },
			};
			const ddl = createTable('cat.sch.f_test', defMixed, columnConfig, escapeId);
			expect(ddl).to.include('`orderid`');
			expect(ddl).to.not.include('OrderId');
		});

		it('maps decimal (no precision) → DECIMAL(18,0)', () => {
			const def = {
				isDimension: false,
				structure: { 'amount': { type: 'decimal' } },
			};
			const ddl = createTable('cat.sch.f_test', def, columnConfig, escapeId);
			expect(ddl).to.include('DECIMAL(18,0)');
		});
	});

	describe('alterAddColumn', () => {
		it('emits ALTER TABLE ADD COLUMN', () => {
			const ddl = alterAddColumn('cat.sch.d_order', 'extra_col', 'varchar(50)', escapeId);
			expect(ddl).to.equal('ALTER TABLE cat.sch.d_order ADD COLUMN `extra_col` STRING');
		});
	});

	describe('alterColumnType', () => {
		it('emits ALTER COLUMN TYPE', () => {
			const ddl = alterColumnType('cat.sch.d_order', 'cost', 'bigint', escapeId);
			expect(ddl).to.equal('ALTER TABLE cat.sch.d_order ALTER COLUMN `cost` TYPE BIGINT');
		});
	});

	describe('mergeFact', () => {
		const nks = ['id'];
		const dataCols = ['channel', 'cost', 'archived'];

		it('emits MERGE INTO ... USING ... ON', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, null, null, escapeId);
			expect(m).to.include('MERGE INTO cat.sch.f_order AS target');
			expect(m).to.include('USING `staging_f_order` AS staging');
			expect(m).to.include('ON (target.`id` = staging.`id`)');
		});

		it('emits WHEN MATCHED UPDATE with COALESCE', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, null, null, escapeId);
			expect(m).to.include('WHEN MATCHED THEN UPDATE SET');
			expect(m).to.include('COALESCE(staging.`channel`, target.`channel`)');
		});

		it('sets _deleted=false and _auditdate in UPDATE', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, null, null, escapeId);
			expect(m).to.include('`_deleted` = false');
			expect(m).to.include('`_auditdate` = staging.`_auditdate`');
		});

		it('emits WHEN NOT MATCHED INSERT', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, null, null, escapeId);
			expect(m).to.include('WHEN NOT MATCHED THEN INSERT');
		});

		it('adds clusterKey filter to ON clause when naturalKeyFilter provided', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, 'id', '1000', escapeId);
			expect(m).to.include('AND target.`id` >= 1000');
		});

		it('omits clusterKey filter when naturalKeyFilter is null', () => {
			const m = mergeFact('cat.sch.f_order', '`staging_f_order`', nks, dataCols, columnConfig, 'id', null, escapeId);
			expect(m).to.not.include('>=');
		});

		it('handles composite natural keys', () => {
			const m = mergeFact('cat.sch.f_test', '`staging_f_test`', ['a', 'b'], ['c'], columnConfig, null, null, escapeId);
			expect(m).to.include('target.`a` = staging.`a` AND target.`b` = staging.`b`');
		});
	});
});

'use strict';

// Basic type map from dw_fields types to Databricks DDL types,
// for use only by the mapType function which has additional behavior.
// Audit of all cloned producer repos found only these scalar types and varchar(n) —
// no super/json columns.
//
// Why `timestamp` and `timestamptz` map to DIFFERENT Databricks types:
//
//   timestamp   → TIMESTAMP_NTZ  — Redshift `TIMESTAMP` is zone-naive. The stored
//                                  values are wall-clocks whose intended timezone
//                                  varies by source (per-source convention, not
//                                  carried in the column). NTZ matches that
//                                  semantics. See ../CLAUDE.md "Timestamp handling
//                                  — preserving legacy no-TZ semantics" and
//                                  data-warehouse/docs/timezone-data-conventions.md
//                                  for the per-source breakdown.
//
//   timestamptz → TIMESTAMP      — Redshift `TIMESTAMPTZ` carries an explicit offset
//                                  (stored as UTC internally). Databricks `TIMESTAMP`
//                                  is also zone-aware (stored as UTC, rendered in
//                                  session TZ). The mapping preserves the offset's
//                                  meaning across joins, comparisons, and time math.
//
// Do not collapse these into a single mapping. Different types at the source mean
// different things and should remain distinct at the target:
//   - timestamptz → TIMESTAMP_NTZ would silently drop the offset, defeating the
//     producer's deliberate choice to carry zone information
//   - timestamp   → TIMESTAMP    would force a UTC interpretation on data that is
//     not UTC (Pacific `d_order.created_at` values would shift on read)
//
// No producer in the current dw_fields audit uses `timestamptz`. The branch is
// kept defined and tested so the right behavior is wired up if one appears, rather
// than falling through to STRING.
//
// Databricks SQL keywords (including type names) are case-insensitive — `int` and
// `INT` both work. Uppercase here is stylistic, for readability of generated DDL.
const TYPE_MAP = {
	'boolean': 'BOOLEAN',
	'date': 'DATE',
	'float': 'FLOAT',
	'int': 'INT',
	'integer': 'INT',
	'bigint': 'BIGINT',
	'timestamp': 'TIMESTAMP_NTZ',
	'timestamptz': 'TIMESTAMP',
};

function mapType(rawType) {
	if (!rawType) return 'STRING';
	const t = rawType.trim().toLowerCase();

	if (TYPE_MAP[t]) return TYPE_MAP[t];

	// varchar(n) → STRING (Databricks has no length-bounded string type)
	if (t.startsWith('varchar')) return 'STRING';

	// decimal with no precision → DECIMAL(18,0) to match Redshift default.
	// Databricks default is DECIMAL(10,0) — do NOT rely on it.
	if (t === 'decimal') return 'DECIMAL(18,0)';

	// decimal(p,s) → pass through verbatim, uppercased
	if (t.startsWith('decimal(')) return rawType.trim().toUpperCase();

	return 'STRING';
}

// Emit a column definition string using the client's escapeId for identifier quoting.
function colDef(name, rawType, escapeId) {
	return `${escapeId(name)} ${mapType(rawType)}`;
}

/**
 * Generate CREATE TABLE IF NOT EXISTS DDL for a dw_fields table definition.
 * Always appends _auditdate TIMESTAMP_NTZ. Facts also get _deleted BOOLEAN;
 * dimensions instead get _startdate / _enddate / _current — mirrored from the
 * postgres connector for schema parity (createTable branches at the same
 * `if (definition.isDimension)` block). Note: SCD is bypassed in all production
 * bot configs (bypassSlowlyChangingDimensions=true, no `scds` fields in any
 * dw_fields), so these columns will be null until dim upsert is implemented.
 * Appends CLUSTER BY (clusterKey) when clusterKey is set.
 *
 * @param {string} qualifiedTable  - fully-qualified table name (catalog.schema.table)
 * @param {object} definition      - dw_fields entry (structure, isDimension, clusterKey)
 * @param {object} columnConfig    - audit column name overrides
 * @param {function} escapeId      - identifier quoting function from connect.js
 * @returns {string} SQL DDL
 */
function createTable(qualifiedTable, definition, columnConfig, escapeId) {
	const cols = [];

	Object.keys(definition.structure).forEach(key => {
		let field = definition.structure[key];
		if (field === 'sk') {
			// Surrogate key: bigint (hashed surrogate keys always used)
			cols.push(`${escapeId(key)} BIGINT`);
			return;
		}
		if (typeof field === 'string') {
			field = { type: field };
		}
		if (!field.type) return;
		cols.push(colDef(key, field.type, escapeId));

		// FK surrogate-key column for dimension links
		if (field.dimension && typeof columnConfig.dimColumnTransform === 'function') {
			const dim = field.dimension;
			const dest = columnConfig.dimColumnTransform(key, field);
			if (columnConfig.useSurrogateDateKeys &&
				(dim === 'd_datetime' || dim === 'datetime' || dim === 'dim_datetime')) {
				cols.push(`${escapeId(dest + '_date')} INT`);
				cols.push(`${escapeId(dest + '_time')} INT`);
			} else if (columnConfig.useSurrogateDateKeys &&
					(dim === 'd_date' || dim === 'date' || dim === 'dim_date')) {
				cols.push(`${escapeId(dest + '_date')} INT`);
			} else if (columnConfig.useSurrogateDateKeys &&
					(dim === 'd_time' || dim === 'time' || dim === 'dim_time')) {
				cols.push(`${escapeId(dest + '_time')} INT`);
			} else {
				cols.push(`${escapeId(dest)} BIGINT`);
			}
		}
	});

	cols.push(`${escapeId(columnConfig._auditdate)} TIMESTAMP_NTZ`);
	if (definition.isDimension) {
		cols.push(`${escapeId(columnConfig._startdate)} TIMESTAMP_NTZ`);
		cols.push(`${escapeId(columnConfig._enddate)} TIMESTAMP_NTZ`);
		cols.push(`${escapeId(columnConfig._current)} BOOLEAN`);
	} else {
		cols.push(`${escapeId(columnConfig._deleted)} BOOLEAN`);
	}
	cols.push(`${escapeId(columnConfig._rescued_data)} STRING`);

	let sql = `CREATE TABLE IF NOT EXISTS ${qualifiedTable} (\n  ${cols.join(',\n  ')}\n) USING DELTA`;

	if (definition.clusterKey) {
		sql += `\nCLUSTER BY (${escapeId(definition.clusterKey)})`;
	}

	return sql;
}

/**
 * Generate ALTER TABLE ... ADD COLUMN DDL.
 */
function alterAddColumn(qualifiedTable, columnName, rawType, escapeId) {
	return `ALTER TABLE ${qualifiedTable} ADD COLUMN ${escapeId(columnName)} ${mapType(rawType)}`;
}

/**
 * Generate ALTER TABLE ... ALTER COLUMN ... TYPE DDL.
 * Throws if attempting to narrow a type (Databricks only widens).
 */
function alterColumnType(qualifiedTable, columnName, newRawType, escapeId) {
	return `ALTER TABLE ${qualifiedTable} ALTER COLUMN ${escapeId(columnName)} TYPE ${mapType(newRawType)}`;
}

/**
 * Build a MERGE INTO statement for fact tables.
 *
 * UPDATE sets each data column to COALESCE(staging.x, target.x), _deleted=false, _auditdate.
 * INSERT adds all columns.
 *
 * @param {string} target           - fully-qualified target table
 * @param {string} staging          - temporary view name
 * @param {string[]} nks            - natural key column names
 * @param {string[]} dataCols       - non-NK, non-audit data columns
 * @param {object} columnConfig     - audit column name overrides
 * @param {string|null} clusterKey  - reserved, ignored (was incorrectly used as MERGE ON filter)
 * @param {string|null} naturalKeyFilter - reserved, ignored (was incorrectly used as MERGE ON filter)
 * @param {function} escapeId       - identifier quoting function
 * @returns {string} MERGE SQL
 */
function mergeFact(target, staging, nks, dataCols, columnConfig, clusterKey, naturalKeyFilter, escapeId) {
	const ad = escapeId(columnConfig._auditdate);
	const del = escapeId(columnConfig._deleted);
	const rd = escapeId(columnConfig._rescued_data);

	const nkMatch = nks.map(k => `target.${escapeId(k)} = staging.${escapeId(k)}`).join(' AND ');

	const updateSets = dataCols.map(c => `${escapeId(c)} = COALESCE(staging.${escapeId(c)}, target.${escapeId(c)})`);
	updateSets.push(`${del} = false`);
	updateSets.push(`${ad} = staging.${ad}`);
	updateSets.push(`${rd} = staging.${rd}`);

	const allCols = [...nks, ...dataCols, columnConfig._auditdate, columnConfig._deleted, columnConfig._rescued_data];
	const insertCols = allCols.map(c => escapeId(c)).join(', ');
	const insertVals = allCols.map(c => `staging.${escapeId(c)}`).join(', ');

	return [
		`MERGE INTO ${target} AS target`,
		`USING ${staging} AS staging`,
		`ON (${nkMatch})`,
		`WHEN MATCHED THEN UPDATE SET`,
		`  ${updateSets.join(',\n  ')}`,
		`WHEN NOT MATCHED THEN INSERT (${insertCols})`,
		`VALUES (${insertVals})`,
	].join('\n');
}

/**
 * Build a MERGE INTO statement for dimension tables (bypassSlowlyChangingDimensions=true).
 *
 * MATCHED: update data columns + _auditdate only. SCD audit cols (_startdate/_enddate/_current)
 *          are left untouched — they preserve the target row's original values.
 * NOT MATCHED: insert nks + data cols + _auditdate from staging; hard-code sentinel values
 *              that match the postgres bypass path:
 *                _current   = true
 *                _startdate = '1900-01-01 00:00:00'
 *                _enddate   = '9999-01-01 00:00:00'
 *
 * Sentinel values mirror connectors/postgres/lib/dwconnect.js:638-642 so that the
 * Step 12 equivalence check (MD5 row-level diff vs Redshift) passes for new dim rows.
 * "active current row" consumers filter on _current=true and _enddate='9999-01-01' —
 * diverging from these values would silently break every downstream join.
 *
 * @param {string} target           - fully-qualified target table
 * @param {string} staging          - staging expression (inline read_files() SELECT)
 * @param {string[]} nks            - natural key column names
 * @param {string[]} dataCols       - non-NK, non-audit data columns
 * @param {object} columnConfig     - audit column name overrides
 * @param {string|null} clusterKey  - reserved, ignored (was incorrectly used as MERGE ON filter)
 * @param {string|null} naturalKeyFilter - reserved, ignored (was incorrectly used as MERGE ON filter)
 * @param {function} escapeId       - identifier quoting function
 * @returns {string} MERGE SQL
 */
function mergeDim(target, staging, nks, dataCols, columnConfig, clusterKey, naturalKeyFilter, escapeId) {
	const ad = escapeId(columnConfig._auditdate);
	const rd = escapeId(columnConfig._rescued_data);

	const nkMatch = nks.map(k => `target.${escapeId(k)} = staging.${escapeId(k)}`).join(' AND ');

	// MATCHED: update only data cols + _auditdate; leave _startdate/_enddate/_current intact.
	const updateSets = dataCols.map(c => `${escapeId(c)} = COALESCE(staging.${escapeId(c)}, target.${escapeId(c)})`);
	updateSets.push(`${ad} = staging.${ad}`);
	updateSets.push(`${rd} = staging.${rd}`);

	// NOT MATCHED: staging provides nks + dataCols + _auditdate + _rescued_data; sentinel values hard-coded.
	const payloadCols = [...nks, ...dataCols, columnConfig._auditdate, columnConfig._rescued_data];
	const insertCols = [
		...payloadCols.map(c => escapeId(c)),
		escapeId(columnConfig._current),
		escapeId(columnConfig._startdate),
		escapeId(columnConfig._enddate),
	].join(', ');
	const insertVals = [
		...payloadCols.map(c => `staging.${escapeId(c)}`),
		'true',
		"'1900-01-01 00:00:00'",
		"'9999-01-01 00:00:00'",
	].join(', ');

	return [
		`MERGE INTO ${target} AS target`,
		`USING ${staging} AS staging`,
		`ON (${nkMatch})`,
		`WHEN MATCHED THEN UPDATE SET`,
		`  ${updateSets.join(',\n  ')}`,
		`WHEN NOT MATCHED THEN INSERT (${insertCols})`,
		`VALUES (${insertVals})`,
	].join('\n');
}

module.exports = { mapType, createTable, alterAddColumn, alterColumnType, mergeFact, mergeDim };

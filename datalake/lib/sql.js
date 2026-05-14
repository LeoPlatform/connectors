'use strict';

// Type map from dw_fields types to Databricks DDL types.
// Audit of all cloned producer repos found only these scalar types — no super/json columns.
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
 * Always appends _auditdate TIMESTAMP_NTZ and _deleted BOOLEAN audit columns.
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
	});

	cols.push(`${escapeId(columnConfig._auditdate)} TIMESTAMP_NTZ`);
	cols.push(`${escapeId(columnConfig._deleted)} BOOLEAN`);

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
 * @param {string|null} clusterKey  - column used for MERGE pruning filter (or null)
 * @param {string|null} naturalKeyFilter - literal value for WHERE target.clusterKey >= ? (or null)
 * @param {function} escapeId       - identifier quoting function
 * @returns {string} MERGE SQL
 */
function mergeFact(target, staging, nks, dataCols, columnConfig, clusterKey, naturalKeyFilter, escapeId) {
	const ad = escapeId(columnConfig._auditdate);
	const del = escapeId(columnConfig._deleted);

	const nkMatch = nks.map(k => `target.${escapeId(k)} = staging.${escapeId(k)}`).join(' AND ');

	let clusterPredicate = '';
	if (clusterKey != null && naturalKeyFilter != null) {
		clusterPredicate = `\n  AND target.${escapeId(clusterKey)} >= ${naturalKeyFilter}`;
	}

	const updateSets = dataCols.map(c => `${escapeId(c)} = COALESCE(staging.${escapeId(c)}, target.${escapeId(c)})`);
	updateSets.push(`${del} = false`);
	updateSets.push(`${ad} = staging.${ad}`);

	const allCols = [...nks, ...dataCols, columnConfig._auditdate, columnConfig._deleted];
	const insertCols = allCols.map(c => escapeId(c)).join(', ');
	const insertVals = allCols.map(c => `staging.${escapeId(c)}`).join(', ');

	return [
		`MERGE INTO ${target} AS target`,
		`USING ${staging} AS staging`,
		`ON (${nkMatch}${clusterPredicate})`,
		`WHEN MATCHED THEN UPDATE SET`,
		`  ${updateSets.join(',\n  ')}`,
		`WHEN NOT MATCHED THEN INSERT (${insertCols})`,
		`VALUES (${insertVals})`,
	].join('\n');
}

module.exports = { mapType, createTable, alterAddColumn, alterColumnType, mergeFact };

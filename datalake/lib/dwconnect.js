'use strict';

const async = require('async');
// leo-sdk.streams is the canonical Leo stream module (cf. connect.js / postgres connector).
const ls = require('leo-sdk').streams;
const logger = require('leo-logger');
const sql = require('./sql.js');
const fingerprint64 = require('./surrogate_key.js');

const naiveIsoNow = require('./audit_timestamp.js');

module.exports = function(dbconfig, options) {
	const connect = require('./connect.js');
	const client = connect(dbconfig);
	const dwClient = client;

	const columnConfig = Object.assign({
		_auditdate: '_auditdate',
		_current: '_current',
		_deleted: '_deleted',
		_enddate: '_enddate',
		_startdate: '_startdate',
		dimColumnTransform: (column, field) => {
			field = field || {};
			let dimCol = field[`dim_column${column.replace(field.id || '', '')}`];
			if (dimCol) return dimCol;
			return field.dim_column ? field.dim_column : `d_${column.replace(/_id$/, '').replace(/^d_/, '')}`;
		},
		stageSchema: 'default',
		stageTablePrefix: 'staging',
		useSurrogateDateKeys: true,
	}, options || {});

	client.getDimensionColumn = columnConfig.dimColumnTransform;
	client.columnConfig = columnConfig;

	// ── Temp table / view tracking ─────────────────────────────────────────
	// Temp views auto-drop with the session, so dropTempTables is a no-op.
	client.dropTempTables = async () => true;

	// ── Audit date ─────────────────────────────────────────────────────────
	client.setAuditdate = () => {
		client.auditdate = "'" + naiveIsoNow() + "'";
	};
	client.setAuditdate();

	// ── Schema mutation ────────────────────────────────────────────────────
	/**
	 * Diff cached schema against desired dw_fields; emit CREATE TABLE for missing tables,
	 * ALTER TABLE ADD COLUMN for new columns. Returns {tablename: 'Added'|'Modified'|'Unmodified'}.
	 */
	client.changeTableStructure = async function(structures) {
		const tableResults = {};

		const catalog = dbconfig.catalog;
		const schema = dbconfig.schema || 'default';

		await client.describeTables(schema);

		const tasks = Object.keys(structures).map(table => done => {
			tableResults[table] = 'Unmodified';

			client.describeTable(table, schema).then(fields => {
				const fieldLookup = fields.reduce((acc, f) => {
					acc[f.column_name] = f;
					return acc;
				}, {});

				const missingFields = {};
				const def = structures[table];

				if (!def.isDimension) {
					if (!fieldLookup[columnConfig._deleted]) {
						missingFields[columnConfig._deleted] = { type: 'boolean' };
					}
				}

				Object.keys(def.structure).forEach(f => {
					const field = def.structure[f];
					if (f === 'sk') return; // surrogate key emitted in createTable
					if (!(f in fieldLookup)) {
						missingFields[f] = typeof field === 'string' ? { type: field } : field;
					}
				});

				if (!fieldLookup[columnConfig._auditdate]) {
					missingFields[columnConfig._auditdate] = { type: 'timestamp' };
				}

				if (Object.keys(missingFields).length) {
					tableResults[table] = 'Modified';
					const addTasks = Object.keys(missingFields).map(col => addDone => {
						const rawType = (missingFields[col] && missingFields[col].type) || 'varchar(255)';
						const qualifiedTable = `${catalog}.${schema}.${client.escapeId(table).replace(/`/g, '')}`;
						client.query(
							sql.alterAddColumn(qualifiedTable, col, rawType, client.escapeId),
							[], addDone
						);
					});
					async.series(addTasks, done);
				} else {
					done();
				}
			}).catch(err => {
				if (err === 'NO_SCHEMA_FOUND') {
					logger.info('Creating table', table);
					const qualifiedTable = `${catalog}.${schema}.${client.escapeId(table).replace(/`/g, '')}`;
					const ddl = sql.createTable(qualifiedTable, structures[table], columnConfig, client.escapeId);
					client.query(ddl, [], createErr => {
						if (createErr) return done(createErr);
						// Invalidate cache so next describeTable reflects new table
						client.clearSchemaCache();
						tableResults[table] = 'Added';
						done();
					});
				} else {
					done(err);
				}
			});
		});

		return new Promise((resolve, reject) => {
			async.parallelLimit(tasks, 5, err => {
				if (err) return reject(err);
				resolve(tableResults);
			});
		});
	};

	// ── importFact ─────────────────────────────────────────────────────────
	/**
	 * Stage stream to S3, mount as temp view, MERGE INTO Delta table.
	 * Signature matches connectors/postgres/lib/dwconnect.js:128 so load.js works unchanged.
	 */
	client.importFact = function(stream, table, ids, callback, tableDef) {
		if (!Array.isArray(ids)) ids = [ids];
		tableDef = tableDef || {};

		const catalog = dbconfig.catalog;
		const schema = dbconfig.schema || 'default';
		const qualifiedTable = `${catalog}.${schema}.${client.escapeId(table).replace(/`/g, '')}`;

		// Stream-level delete handler: collect __leo_delete__ records separately.
		const deleteRecords = [];
		const dataStream = ls.through((obj, done, _push) => {
			if (obj.__leo_delete__) {
				deleteRecords.push(obj);
				done();
			} else {
				done(null, obj);
			}
		});

		client.describeTable(table, schema).then(tableFields => {
			const allCols = tableFields.map(f => f.column_name);
			const columnDefs = tableFields.map(f => ({ name: f.column_name, type: reconstructType(f) }));
			const fieldLookup = tableFields.reduce((acc, f) => {
				acc[f.column_name.toLowerCase()] = f;
				return acc;
			}, {});
			const nks = ids;
			const auditCol = columnConfig._auditdate;
			const delCol = columnConfig._deleted;

			// Compute natural-key lower bound for MERGE pruning (hashedSurrogateKeys path).
			// clusterKey from tableDef takes precedence; fall back to first NK.
			const clusterKey = tableDef.clusterKey || null;
			const pruneCol = clusterKey || (ids.length === 1 ? ids[0] : null);

			// Enrich each row: add auditdate, _deleted=false, compute surrogate key.
			const enrichedStream = ls.through((obj, done) => {
				// Compute surrogate key if schema has a sk column
				const skField = tableDef.structure && Object.keys(tableDef.structure).find(k => {
					const f = tableDef.structure[k];
					return f === 'sk' || (f && f.sk);
				});
				if (skField) {
					const nkValues = nks.map(k => obj[k]);
					obj[skField] = fingerprint64(nkValues);
				}
				obj[auditCol] = dwClient.auditdate ? dwClient.auditdate.replace(/'/g, '') : naiveIsoNow();
				obj[delCol] = false;
				done(null, obj);
			});

			// Resolve staging location via the connector's single source of truth.
			// ensureStagingLocation pins client.s3Bucket / client.s3Prefix; downstream
			// staging reads from the client without re-resolving.
			client.ensureStagingLocation().then(() => {
				const stageStream = client.streamToTableFromS3(table, {
					columnDefs: columnDefs,
					auditdate: dwClient.auditdate,
				});

				ls.pipe(stream, dataStream, enrichedStream, stageStream, err => {
					if (err) return callback(err);

					// Build an inline read_files(...) SELECT for the staged S3 file. Each
					// client.query() opens its own session — a session-scoped temp view
					// would not survive across the MIN and MERGE queries, so we inline.
					if (!client.lastStagingS3Uri || !client.lastStagingColumnDefs) {
						return callback(new Error('staging did not produce an S3 URI; cannot build staging SELECT'));
					}
					const stagingSelect = client.buildStagingSelect(client.lastStagingS3Uri, client.lastStagingColumnDefs);
					const stagingClause = `(\n${stagingSelect}\n)`;

					flushDeletes(client, qualifiedTable, deleteRecords, ids, columnConfig, dwClient.auditdate, () => {
						let naturalKeyFilter = null;

						const auditCols = new Set([auditCol, delCol, columnConfig._current, columnConfig._startdate, columnConfig._enddate]);
						const dataCols = allCols.filter(c => !nks.includes(c) && !auditCols.has(c));

						const mergeCallback = (mergeErr, mergeResult) => cleanupStagedFile(client, dbconfig, mergeErr, mergeResult, callback);

						if (pruneCol) {
							const minSql = `SELECT MIN(\`${pruneCol.toLowerCase()}\`) AS minval, CAST(COUNT(*) AS INT) AS cnt FROM ${stagingClause} AS staging`;
							const pruneColType = (fieldLookup[pruneCol.toLowerCase()] && fieldLookup[pruneCol.toLowerCase()].data_type) || '';
							client.query(minSql, [], (qErr, results) => {
								if (!qErr && results && results[0] && results[0].minval != null) {
									naturalKeyFilter = literalForType(results[0].minval, pruneColType, client.escapeValueNoToLower);
								}
								doMerge(client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, clusterKey, naturalKeyFilter, mergeCallback);
							});
						} else {
							doMerge(client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, clusterKey, null, mergeCallback);
						}
					});
				});
			}).catch(callback);
		}).catch(callback);
	};

	// ── Dim stubs (surface expected by load.js:224-316) ────────────────────
	client.importDimension = function(stream, table, sk, nk, scds, callback) {
		callback(new Error('importDimension not yet implemented for Databricks connector'));
	};

	client.insertMissingDimensions = function(usedTables, tableConfig, tableSks, tableNks, callback) {
		callback(new Error('insertMissingDimensions not yet implemented for Databricks connector'));
	};

	client.linkDimensions = function(table, links, nk, done) {
		done(new Error('linkDimensions not yet implemented for Databricks connector'));
	};

	return client;
};

// ── Helpers ────────────────────────────────────────────────────────────────

// information_schema.columns.data_type reports DECIMAL without (precision, scale).
// Reconstruct from numeric_precision / numeric_scale so the staging read_files
// schema clause matches the target column type exactly — otherwise DECIMAL
// defaults to DECIMAL(10,0) and silently truncates fractional values.
function reconstructType(field) {
	const t = String(field.data_type || '').toUpperCase();
	if (t === 'DECIMAL') {
		const p = field.numeric_precision != null ? field.numeric_precision : 18;
		const s = field.numeric_scale != null ? field.numeric_scale : 0;
		return `DECIMAL(${p},${s})`;
	}
	return t;
}

// Render `value` as a SQL literal appropriate for `dataType`. Numeric columns
// take an unquoted literal; everything else (string, timestamp, date) is
// single-quoted via the connector's standard escaper. Mirrors the type-aware
// branching in ../postgres/lib/dwconnect.js naturalKeyFilter — Databricks
// types replace Postgres int4/int8/varchar/timestamp.
function literalForType(value, dataType, escapeValueNoToLower) {
	const t = String(dataType || '').toUpperCase();
	const isNumeric = /^(BIGINT|INT|INTEGER|SMALLINT|TINYINT|DECIMAL|DOUBLE|FLOAT|REAL|NUMERIC)/.test(t);
	if (isNumeric) {
		return String(value);
	}
	return escapeValueNoToLower(String(value));
}

function flushDeletes(client, qualifiedTable, deleteRecords, ids, columnConfig, auditdate, callback) {
	if (!deleteRecords.length) return callback();

	// Group deletes by the column being deleted
	const byField = {};
	deleteRecords.forEach(obj => {
		const field = obj.__leo_delete__;
		const id = obj.__leo_delete_id__;
		if (id !== undefined) {
			if (!byField[field]) byField[field] = [];
			byField[field].push(id);
		}
	});

	const tasks = Object.keys(byField).map(field => done => {
		const ids = byField[field].map(v => typeof v === 'string' ? `'${v.replace(/'/g, "\\'")}'` : v).join(',');
		const updateSql = `UPDATE ${qualifiedTable} SET \`${columnConfig._deleted}\` = true, \`${columnConfig._auditdate}\` = ${auditdate} WHERE \`${field.toLowerCase()}\` IN (${ids})`;
		client.query(updateSql, [], done);
	});

	async.series(tasks, callback);
}

function doMerge(client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, clusterKey, naturalKeyFilter, callback) {
	const mergeSql = sql.mergeFact(
		qualifiedTable,
		stagingClause,
		nks,
		dataCols,
		columnConfig,
		clusterKey,
		naturalKeyFilter,
		client.escapeId
	);

	client.query(mergeSql, [], (err, results) => {
		callback(err, results ? { count: (results && results.length) || 0 } : { count: 0 });
	});
}

function cleanupStagedFile(client, dbconfig, mergeErr, mergeResult, callback) {
	if (dbconfig.keepS3Files) {
		return callback(mergeErr, mergeResult);
	}
	const staging = client.lastStagingS3;
	if (!staging) {
		return callback(mergeErr, mergeResult);
	}
	// Same S3 client leo-sdk's streams.toS3 used for the upload (default credential chain).
	const s3 = require('leo-sdk').aws.s3;
	s3.deleteObject({ Bucket: staging.bucket, Key: staging.key }, deleteErr => {
		if (deleteErr) {
			logger.info('staged file delete failed:', staging.key, deleteErr);
		}
		callback(mergeErr, mergeResult);
	});
}

// Exposed for unit testing of the type-aware naturalKeyFilter quoting.
module.exports.literalForType = literalForType;

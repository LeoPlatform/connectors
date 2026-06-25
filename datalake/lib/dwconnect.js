'use strict';

const async = require('async');
// leo-sdk.streams is the canonical Leo stream module (cf. connect.js / postgres connector).
const ls = require('leo-sdk').streams;
const logger = require('leo-logger');
const sql = require('./sql.js');
const fingerprint64 = require('./surrogate_key.js');
const connect = require('./connect.js');
const { isConnectionError } = connect;

const naiveIsoNow = require('./audit_timestamp.js');

module.exports = function(dbconfig, options) {
	const client = connect(dbconfig);
	const dwClient = client;

	const columnConfig = Object.assign({
		_auditdate: '_auditdate',
		_current: '_current',
		_deleted: '_deleted',
		_enddate: '_enddate',
		_rescued_data: '_rescued_data',
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
					// FK columns for dimension links
					if (field && typeof field === 'object' && field.dimension) {
						const dim = field.dimension;
						const dest = columnConfig.dimColumnTransform(f, field);
						if (columnConfig.useSurrogateDateKeys &&
							(dim === 'd_datetime' || dim === 'datetime' || dim === 'dim_datetime')) {
							if (!(dest + '_date' in fieldLookup)) missingFields[dest + '_date'] = { type: 'integer' };
							if (!(dest + '_time' in fieldLookup)) missingFields[dest + '_time'] = { type: 'integer' };
						} else if (columnConfig.useSurrogateDateKeys &&
								(dim === 'd_date' || dim === 'date' || dim === 'dim_date')) {
							if (!(dest + '_date' in fieldLookup)) missingFields[dest + '_date'] = { type: 'integer' };
						} else if (columnConfig.useSurrogateDateKeys &&
								(dim === 'd_time' || dim === 'time' || dim === 'dim_time')) {
							if (!(dest + '_time' in fieldLookup)) missingFields[dest + '_time'] = { type: 'integer' };
						} else if (!(dest in fieldLookup)) {
							missingFields[dest] = { type: 'bigint' };
						}
					}
				});

				if (!fieldLookup[columnConfig._auditdate]) {
					missingFields[columnConfig._auditdate] = { type: 'timestamp' };
				}

				if (!fieldLookup[columnConfig._rescued_data]) {
					missingFields[columnConfig._rescued_data] = { type: 'string' };
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
					async.series(addTasks, (err) => {
						if (!err) client.clearSchemaCache();
						done(err);
					});
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

		// Collect __leo_delete__ records; non-delete records flow through to staging.
		const deleteRecords = [];
		const dataStream = ls.through((obj, done, _push) => {
			if (obj.__leo_delete__) {
				deleteRecords.push(obj);
				done();
			} else {
				done(null, obj);
			}
		});

		const nks = ids;
		const auditCol = columnConfig._auditdate;
		const delCol = columnConfig._deleted;

		// Resolve the surrogate-key column once — tableDef.structure is fixed
		// for the duration of this importFact call.
		const skField = tableDef.structure && Object.keys(tableDef.structure).find(k => {
			const f = tableDef.structure[k];
			return f === 'sk' || (f && f.sk);
		});

		const auditdate = dwClient.auditdate;
		const auditdateValue = auditdate ? auditdate.replace(/'/g, '') : naiveIsoNow();

		let stagingCount = 0;
		const fkEnrich = buildFkEnrichers(tableDef && tableDef.structure, columnConfig);
		const enrichFn = obj => {
			stagingCount++;
			if (skField) {
				obj[skField] = fingerprint64(nks.map(k => obj[k]));
			}
			fkEnrich(obj);
			obj[auditCol] = auditdateValue;
			obj[delCol] = false;
		};

		// importFact owns the staging-path identifier — same pattern as postgres'
		// importFact owning qualifiedStagingTable. stageToS3 computes it and passes
		// it back; no back-channel through shared client state.
		stageToS3(client, table, [stream, dataStream], dbconfig, enrichFn, auditdate, (err, staged) => {
			if (err) return callback(err);

			const { stagingPath, stagingClause, allCols, fieldLookup } = staged;
			const auditCols = new Set([auditCol, delCol, columnConfig._current, columnConfig._startdate, columnConfig._enddate, columnConfig._rescued_data]);
			const dataCols = allCols.filter(c => !nks.includes(c) && !auditCols.has(c));

			const mergeCallback = (mergeErr) =>
				cleanupStagedFile(dbconfig, stagingPath, mergeErr, mergeErr ? null : { count: stagingCount }, callback);

			withRetry(done => flushDeletes(client, qualifiedTable, deleteRecords, ids, columnConfig, auditdate, fieldLookup, done), {}, (flushErr) => {
				if (flushErr) return mergeCallback(flushErr);
				withRetry(done => doMerge(sql.mergeFact, client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, done), {}, mergeCallback);
			});
		});
	};

	// ── Dim upsert ─────────────────────────────────────────────────────────
	/**
	 * Stage a dimension stream to S3, then MERGE INTO the Delta dim table.
	 * bypassSlowlyChangingDimensions=true in all production configs — no SCD2 logic.
	 * Sentinel values for new rows match the postgres bypass path:
	 *   _current=true, _startdate='1900-01-01 00:00:00', _enddate='9999-01-01 00:00:00'.
	 * Signature matches connectors/postgres/lib/dwconnect.js:363 so load.js works unchanged.
	 */
	client.importDimension = function(stream, table, sk, nk, scds, callback, tableDef) {
		const nks = Array.isArray(nk) ? nk : [nk];
		tableDef = tableDef || {};

		const catalog = dbconfig.catalog;
		const schema = dbconfig.schema || 'default';
		const qualifiedTable = `${catalog}.${schema}.${client.escapeId(table).replace(/`/g, '')}`;

		// Collect __leo_delete__ markers for soft-close after staging.
		// id-marked deletes are also pushed to staging (mirrors postgres importDimension).
		const deleteRecords = [];
		const dataStream = ls.through((obj, done, push) => {
			if (obj.__leo_delete__) {
				if (obj.__leo_delete__ === 'id') {
					push(obj);
				}
				deleteRecords.push(obj);
				done();
			} else {
				done(null, obj);
			}
		});

		const auditCol = columnConfig._auditdate;

		const skField = tableDef.structure && Object.keys(tableDef.structure).find(k => {
			const f = tableDef.structure[k];
			return f === 'sk' || (f && f.sk);
		});

		const auditdate = dwClient.auditdate;
		const auditdateValue = auditdate ? auditdate.replace(/'/g, '') : naiveIsoNow();

		let stagingCount = 0;
		const fkEnrich = buildFkEnrichers(tableDef && tableDef.structure, columnConfig);
		const enrichFn = obj => {
			if (!obj.__leo_delete__) stagingCount++;
			if (skField) {
				obj[skField] = fingerprint64(nks.map(k => obj[k]));
			}
			fkEnrich(obj);
			obj[auditCol] = auditdateValue;
		};

		stageToS3(client, table, [stream, dataStream], dbconfig, enrichFn, auditdate, (err, staged) => {
			if (err) return callback(err);

			const { stagingPath, stagingClause, allCols, fieldLookup } = staged;
			// _deleted is not an audit column for dims; _current/_startdate/_enddate
			// are managed by the MERGE SQL (sentinel values on INSERT; preserved on UPDATE).
			const auditCols = new Set([auditCol, columnConfig._current, columnConfig._startdate, columnConfig._enddate, columnConfig._rescued_data]);
			const dataCols = allCols.filter(c => !nks.includes(c) && !auditCols.has(c));

			const mergeCallback = (mergeErr) =>
				cleanupStagedFile(dbconfig, stagingPath, mergeErr, mergeErr ? null : { count: stagingCount }, callback);

			withRetry(done => flushDimDeletes(client, qualifiedTable, deleteRecords, columnConfig, auditdate, fieldLookup, done), {}, (flushErr) => {
				if (flushErr) return mergeCallback(flushErr);
				withRetry(done => doMerge(sql.mergeDim, client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, done), {}, mergeCallback);
			});
		});
	};

	// insertMissingDimensions: intentional no-op. Under hashedSurrogateKeys=true,
	// postgres/lib/dwconnect.js:680 immediately calls callback(null) for the same
	// reason — hashed surrogate keys make stub placeholder rows unnecessary, because
	// any FK reference that arrives before its dimension row will compute the same
	// hash and merge correctly when the dim row appears. This connector always uses
	// hashedSurrogateKeys, so this is a deliberate no-op, not a deferred feature.
	client.insertMissingDimensions = function(usedTables, tableConfig, tableSks, tableNks, callback) {
		callback(null);
	};

	// linkDimensions: FK surrogate-key values are pre-computed in the importFact /
	// importDimension enrichFns (via buildFkEnrichers) and written into the staging CSV
	// before the MERGE — no post-MERGE SQL update is needed. This diverges from postgres,
	// where FARMFINGERPRINT64() in an UPDATE handles this; Databricks SQL has no
	// FARMFINGERPRINT64 equivalent, so the computation moves to Node.js. See
	// docs/porting-decisions.md and docs/BUILD_PLAN.md §Step 6 extension.
	client.linkDimensions = function(table, links, nk, done) {
		done(null);
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

function flushDeletes(client, qualifiedTable, deleteRecords, ids, columnConfig, auditdate, fieldLookup, callback) {
	if (!deleteRecords.length) return callback();

	// Group deletes by the column being deleted; skip records whose __leo_delete__
	// value is not a real column (mirrors postgres deletesSetup colLookup[field] gate).
	const byField = {};
	deleteRecords.forEach(obj => {
		const field = obj.__leo_delete__;
		const id = obj.__leo_delete_id__;
		if (id !== undefined && fieldLookup[field.toLowerCase()]) {
			if (!byField[field]) byField[field] = [];
			byField[field].push(id);
		}
	});

	const tasks = Object.keys(byField).map(field => done => {
		const ids = byField[field].map(v => typeof v === 'string' ? client.escape(v) : v).join(',');
		const updateSql = `UPDATE ${qualifiedTable} SET \`${columnConfig._deleted}\` = true, \`${columnConfig._auditdate}\` = ${auditdate} WHERE \`${field.toLowerCase()}\` IN (${ids})`;
		client.query(updateSql, [], done);
	});

	async.series(tasks, callback);
}

function flushDimDeletes(client, qualifiedTable, deleteRecords, columnConfig, auditdate, fieldLookup, callback) {
	if (!deleteRecords.length) return callback();

	// Same column-existence gate as flushDeletes and postgres deletesSetup.
	const byField = {};
	deleteRecords.forEach(obj => {
		const field = obj.__leo_delete__;
		const id = obj.__leo_delete_id__;
		if (id !== undefined && fieldLookup[field.toLowerCase()]) {
			if (!byField[field]) byField[field] = [];
			byField[field].push(id);
		}
	});

	const tasks = Object.keys(byField).map(field => done => {
		const ids = byField[field].map(v => typeof v === 'string' ? client.escape(v) : v).join(',');
		const updateSql = `UPDATE ${qualifiedTable} SET \`${columnConfig._enddate}\` = ${auditdate}, \`${columnConfig._auditdate}\` = ${auditdate} WHERE \`${field.toLowerCase()}\` IN (${ids}) AND \`${columnConfig._current}\` = true`;
		client.query(updateSql, [], done);
	});

	async.series(tasks, callback);
}

// Shared S3-staging pipeline: describeTable → enrich rows → stage to S3 → build staging clause.
// Both importFact and importDimension call this; they differ only in their enrichFn and merge SQL.
// pipelinePre is an array of streams to pipe before the enrichedStream, e.g. [rawStream] for dims
// or [rawStream, filterStream] for facts.
function stageToS3(client, table, pipelinePre, dbconfig, enrichFn, auditdate, callback) {
	const schema = dbconfig.schema || 'default';

	client.describeTable(table, schema).then(tableFields => {
		const allCols = tableFields.map(f => f.column_name);
		const columnDefs = tableFields.map(f => ({ name: f.column_name, type: reconstructType(f) }));
		const fieldLookup = tableFields.reduce((acc, f) => {
			acc[f.column_name.toLowerCase()] = f;
			return acc;
		}, {});

		const enrichedStream = ls.through((obj, done) => {
			enrichFn(obj);
			done(null, obj);
		});

		client.ensureStagingLocation().then(() => {
			const stagingPath = client.stagingS3Path(table, auditdate);
			const s3Stage = client.streamToTableFromS3(table, { columnDefs, s3Path: stagingPath });

			ls.pipe(...pipelinePre, enrichedStream, s3Stage, err => {
				if (err) return callback(err);
				const stagingSelect = client.buildStagingSelect(stagingPath.uri, columnDefs);
				const stagingClause = `(\n${stagingSelect}\n)`;
				callback(null, { stagingPath, stagingClause, allCols, fieldLookup });
			});
		}).catch(callback);
	}).catch(callback);
}

function doMerge(mergeSqlFn, client, qualifiedTable, stagingClause, nks, dataCols, columnConfig, callback) {
	const mergeSql = mergeSqlFn(
		qualifiedTable,
		stagingClause,
		nks,
		dataCols,
		columnConfig,
		client.escapeId
	);

	client.query(mergeSql, [], callback);
}

function cleanupStagedFile(dbconfig, stagingPath, mergeErr, mergeResult, callback) {
	if (dbconfig.keepS3Files || !stagingPath) {
		return callback(mergeErr, mergeResult);
	}
	// Same S3 client leo-sdk's streams.toS3 used for the upload (default credential chain).
	const s3 = require('leo-sdk').aws.s3;
	s3.deleteObject({ Bucket: stagingPath.bucket, Key: stagingPath.key }, deleteErr => {
		if (deleteErr) {
			logger.info('staged file delete failed:', stagingPath.key, deleteErr);
		}
		callback(mergeErr, mergeResult);
	});
}

// ── Bounded idempotent retry ──────────────────────────────────────────────────
// Wraps a callback-based function `fn(done)` with up to `opts.attempts` retries
// (default 3). Retries only on connection-class errors — query-class errors
// (SQL syntax, permission, data) propagate immediately. Each retry re-acquires
// a fresh pool session (the dead one was destroyed by query()'s error handler).
// Safe because the callers — COUNT SELECT, MERGE, flushDeletes UPDATE — are all
// idempotent: re-running yields the same final state. Never retry inside query()
// itself (which runs arbitrary, possibly non-idempotent SQL).
function withRetry(fn, opts, callback) {
	const maxAttempts = (opts && opts.attempts) || 3;
	const backoffMs = (opts && opts.backoffMs) || 200;
	let attemptsLeft = maxAttempts;

	function attempt() {
		fn(function(err, result) {
			if (!err) return callback(null, result);
			attemptsLeft--;
			if (attemptsLeft > 0 && isConnectionError(err)) {
				const delay = backoffMs * (maxAttempts - attemptsLeft);
				setTimeout(attempt, delay);
			} else {
				callback(err, result);
			}
		});
	}

	attempt();
}

// ── FK surrogate-key helpers ───────────────────────────────────────────────
// These mirror the FK computations postgres linkDimensions does in SQL using
// FARMFINGERPRINT64 / date arithmetic. Databricks has no FARMFINGERPRINT64 SQL
// function, so the work moves to Node.js inside importFact/importDimension enrichFns.

// Reference timestamp for surrogate date-key arithmetic (ms since Unix epoch).
// Date.UTC avoids any server-local TZ shift on the constant.
const _DATE_1400_UTC_MS = Date.UTC(1400, 0, 1);

// Wall-clock string → surrogate date key: days since 1400-01-01 + 10000.
// Parses by splitting on space or T rather than round-tripping through Date
// constructor, which would apply the server's local TZ to an offset-free string.
function dateSk(wallClock) {
	if (wallClock == null) return 1;
	const dateStr = String(wallClock).split(/[ T]/)[0];
	const parts = dateStr.split('-');
	if (parts.length < 3) return 1;
	const y = parseInt(parts[0], 10), mo = parseInt(parts[1], 10), d = parseInt(parts[2], 10);
	if (isNaN(y) || isNaN(mo) || isNaN(d)) return 1;
	return Math.floor((Date.UTC(y, mo - 1, d) - _DATE_1400_UTC_MS) / 86400000) + 10000;
}

// Wall-clock string → surrogate time key: seconds since midnight + 10000.
function timeSk(wallClock) {
	if (wallClock == null) return 1;
	const timePart = String(wallClock).split(/[ T]/)[1];
	if (!timePart) return 1;
	const parts = timePart.split(':');
	if (parts.length < 3) return 1;
	const h = parseInt(parts[0], 10), m = parseInt(parts[1], 10), s = parseInt(parts[2], 10);
	if (isNaN(h) || isNaN(m) || isNaN(s)) return 1;
	return h * 3600 + m * 60 + s + 10000;
}

// Build a per-object enrichment function that sets FK surrogate-key columns.
// Called once at importFact/importDimension construction time; the returned
// function is invoked on every staged row before CSV write.
//
// Null coalesce: Redshift COALESCE(FARMFINGERPRINT64(NULL), 1) = 1. JS fingerprint64
// coerces null to '' and hashes it (result ≠ 1). Explicitly write '1' for null inputs.
function buildFkEnrichers(structure, columnConfig) {
	if (!structure) return () => {};
	const fns = [];
	Object.keys(structure).forEach(f => {
		const field = structure[f];
		if (!field || typeof field !== 'object' || !field.dimension) return;
		const dim = field.dimension;
		const dest = columnConfig.dimColumnTransform(f, field);
		if (columnConfig.useSurrogateDateKeys &&
			(dim === 'd_datetime' || dim === 'datetime' || dim === 'dim_datetime')) {
			fns.push(obj => {
				const v = obj[f];
				obj[dest + '_date'] = dateSk(v);
				obj[dest + '_time'] = timeSk(v);
			});
		} else if (columnConfig.useSurrogateDateKeys &&
				(dim === 'd_date' || dim === 'date' || dim === 'dim_date')) {
			fns.push(obj => { obj[dest + '_date'] = dateSk(obj[f]); });
		} else if (columnConfig.useSurrogateDateKeys &&
				(dim === 'd_time' || dim === 'time' || dim === 'dim_time')) {
			fns.push(obj => { obj[dest + '_time'] = timeSk(obj[f]); });
		} else {
			fns.push(obj => {
				const v = obj[f];
				obj[dest] = v == null ? '1' : fingerprint64([v]);
			});
		}
	});
	return obj => fns.forEach(fn => fn(obj));
}

// Exposed for unit testing.
module.exports.withRetry = withRetry;
module.exports.dateSk = dateSk;
module.exports.timeSk = timeSk;

'use strict';

const { DBSQLClient } = require('@databricks/sql');
const logger = require('leo-logger')('connector.sql.datalake');
const csv = require('fast-csv');
// leo-sdk.streams is the canonical Leo stream module across LeoPlatform connectors
// (cf. connectors/postgres/lib/dwconnect.js — same pattern). It exposes pipeline,
// through, pipe, AND toS3 — use it for all stream primitives. The separate
// `leo-streams` npm package is a lighter sibling without toS3; we don't import it.
const ls = require('leo-sdk').streams;
const naiveIsoNow = require('./audit_timestamp.js');

module.exports = function(config) {
	// Schema cache mirrors connectors/postgres/lib/connect.js lines 40-135.
	let cache = {
		schema: {},
		timestamp: null,
	};

	const client = {
		// ── Schema cache ──────────────────────────────────────────────────
		clearSchemaCache: () => {
			logger.info('Clearing Tables schema cache');
			cache.schema = {};
		},
		getSchemaCache: () => cache.schema,
		setSchemaCache: (s) => {
			cache.schema = s;
			cache.timestamp = Date.now();
		},

		// ── Connection ────────────────────────────────────────────────────
		connect: async (_opts) => {
			const dbsql = new DBSQLClient();
			// Auth selection: prefer OAuth M2M (service principal) when client_id/client_secret
			// are provided; otherwise use PAT. Local dev uses M2M via ~/.databrickscfg [dev-cup];
			// CI may use PAT in the future.
			const connOpts = (config.clientId && config.clientSecret)
				? {
					host: config.host,
					path: config.path,
					authType: 'databricks-oauth',
					oauthClientId: config.clientId,
					oauthClientSecret: config.clientSecret,
				}
				: {
					host: config.host,
					path: config.path,
					token: config.token,
				};
			await dbsql.connect(connOpts);

			const catalog = config.catalog;
			const schema = config.schema;

			const session = await dbsql.openSession({
				initialCatalog: catalog,
				initialSchema: schema,
				// ansi_mode=false: mirrors Redshift lenient cast/arithmetic semantics during coexistence.
				// timezone=UTC + infer_timestamp_ntz_type=true: paired to avoid a third TZ shift on top
				// of the existing enterprise correction chain. Connector reads/writes only TIMESTAMP_NTZ.
				initialParameters: {
					ansi_mode: 'false',
					infer_timestamp_ntz_type: 'true',
					timezone: 'UTC',
				},
			});

			// Return an isolated sub-client wrapping this session, so callers
			// can release() it without closing the outer connection.
			return createSessionClient(session, cache, config);
		},

		disconnect: async () => {
			// No-op: connection is opened per-operation via client.connect().
		},
		end: async () => {},
		release: () => {},

		// ── Query ─────────────────────────────────────────────────────────
		query: (sql, paramsOrCb, cbOrOpts, opts) => {
			let params, cb;
			if (typeof paramsOrCb === 'function') {
				cb = paramsOrCb;
				params = [];
			} else {
				params = paramsOrCb || [];
				cb = cbOrOpts;
			}

			client.connect().then(conn => {
				conn.query(sql, params, (err, rows, fields) => {
					conn.release();
					cb(err, rows, fields);
				}, opts);
			}).catch(cb);
		},

		// ── Schema describe ───────────────────────────────────────────────
		describeTable: async (table, tableSchema) => {
			return new Promise((resolve, reject) => {
				tableSchema = tableSchema || config.schema || 'default';
				const qualifiedTable = `${tableSchema}.${table}`;
				if (cache.schema[qualifiedTable]) {
					logger.info(`Table "${qualifiedTable}" schema from cache`, cache.timestamp);
					return resolve(cache.schema[qualifiedTable]);
				}
				client.describeTables(tableSchema).then(schema => {
					if (schema && schema[qualifiedTable]) {
						return resolve(schema[qualifiedTable]);
					}
					reject('NO_SCHEMA_FOUND');
				}).catch(reject);
			});
		},

		describeTables: async (tableSchema) => {
			return new Promise((resolve, reject) => {
				tableSchema = tableSchema || config.schema || 'default';
				if (Object.keys(cache.schema || {}).length) {
					logger.info('Tables schema from cache', cache.timestamp);
					return resolve(cache.schema);
				}
				const catalog = config.catalog;
				// Include numeric_precision/numeric_scale so callers can reconstruct
				// DECIMAL(p,s) — data_type alone reports just "DECIMAL" without precision.
				const sql = `SELECT table_name, column_name, data_type, numeric_precision, numeric_scale, is_nullable FROM ${catalog}.information_schema.columns WHERE table_schema = ? ORDER BY ordinal_position ASC`;
				client.query(sql, [tableSchema], (err, result) => {
					if (err) return reject(err);
					let schema = {};
					(result || []).forEach(r => {
						const qualifiedTable = `${tableSchema}.${r.table_name}`;
						if (!schema[qualifiedTable]) schema[qualifiedTable] = [];
						schema[qualifiedTable].push(r);
					});
					cache.schema = schema;
					cache.timestamp = Date.now();
					logger.info('Caching Schema Table', cache.timestamp);
					resolve(cache.schema);
				});
			});
		},

		// ── Identifier quoting ────────────────────────────────────────────
		// Databricks SQL uses backtick quoting. Lowercases all identifiers per the
		// lowercase-everywhere convention (open question #7 in BUILD_PLAN.md).
		escapeId: (name) => {
			return '`' + String(name).toLowerCase().replace(/`/g, '') + '`';
		},

		// Used for literal value escaping in SQL strings (not bind params).
		escape: (value) => {
			if (value && value.replace) {
				return "'" + value.replace(/'/g, "\\'") + "'";
			}
			return value;
		},

		escapeValue: (value) => {
			if (value && value.replace) {
				return "'" + value.replace(/'/g, "\\'").toLowerCase() + "'";
			}
			return value;
		},

		escapeValueNoToLower: (value) => {
			if (value && value.replace) {
				return "'" + value.replace(/'/g, "\\'") + "'";
			}
			return value;
		},

		// ── S3 staging → inline read_files() ──────────────────────────────
		// Stages CSV to S3. The downstream MERGE/MIN queries read it back via an
		// inline `read_files(...)` subquery rather than a CREATE TEMPORARY VIEW —
		// each query() opens its own session, so a session-scoped temp view would
		// be invisible to subsequent queries. See BUILD_PLAN.md Step 7 for the
		// per-option rationale of the read_files arguments.
		streamToTableFromS3: (table, config) => {
			return _streamToTableFromS3(client, table, config);
		},

		// Build the read_files() SELECT for use inline in MIN / MERGE queries.
		// columnDefs is `[{name, type}, ...]` — types must match the target table so
		// MERGE's COALESCE(staging.x, target.x) sees consistent types on both sides.
		buildStagingSelect: (s3Uri, columnDefs) => {
			const schemaStr = columnDefs.map(c => `${c.name} ${c.type}`).join(', ');
			return [
				`SELECT * FROM read_files(`,
				`  '${s3Uri}',`,
				`  format => 'csv',`,
				`  sep => '|',`,
				`  header => false,`,
				`  quote => '"',`,
				`  escape => '"',`,
				`  multiLine => 'true',`,
				`  nullValue => '\\\\N',`,
				`  mode => 'PERMISSIVE',`,
				`  schema => '${schemaStr}'`,
				`)`,
			].join('\n');
		},

		// streamToTable: direct-write path (not used in Databricks; kept for interface parity)
		streamToTable: () => {
			throw new Error('streamToTable is not implemented for Databricks; use streamToTableFromS3');
		},
	};

	function setAuditdate() {
		client.auditdate = "'" + naiveIsoNow() + "'";
	}

	setAuditdate();

	return client;
};

// ── Session-scoped sub-client ─────────────────────────────────────────────────
// Wraps a single DBSQLSession so callers can release() it after use.
function createSessionClient(session, _parentCache, _config) {
	let closed = false;

	const conn = {
		query: async (sql, paramsOrCb, cbOrOpts, _opts) => {
			let params, cb;
			if (typeof paramsOrCb === 'function') {
				cb = paramsOrCb;
				params = [];
			} else {
				params = paramsOrCb || [];
				cb = cbOrOpts;
			}

			try {
				// `ordinalParameters` binds positional `?` placeholders. The previous code
				// passed `parameters:` which is not a recognized option — binds were silently ignored.
				const execOpts = params.length ? { ordinalParameters: params } : {};
				const operation = await session.executeStatement(sql, execOpts);
				const result = await operation.fetchAll();
				await operation.close();
				const fields = result.length ? Object.keys(result[0]).map(name => ({ name })) : [];
				cb(null, result, fields);
			} catch (err) {
				cb(err);
			}
		},

		release: async () => {
			if (!closed) {
				closed = true;
				try { await session.close(); } catch (e) { /* ignore */ }
			}
		},
	};

	return conn;
}

// ── Staging location resolution ───────────────────────────────────────────────
// Preference order:
//   1. Explicit config.s3Bucket + config.s3Prefix (set by caller / test helper)
//   2. UC RootLocation lookup via DESCRIBE SCHEMA EXTENDED (fallback for schemas
//      configured with a managed storage location)
// The UC fallback uses the `RootLocation` row, not `Location` (UUID-suffixed
// internal path) or the catalog `Storage Root` (managed-table area, not a
// staging convention).
function _ensureStagingLocation(client, config) {
	if (client._stagingLocation) return Promise.resolve(client._stagingLocation);

	if (config.s3Bucket && config.s3Prefix) {
		client._stagingLocation = {
			s3Bucket: config.s3Bucket,
			s3Prefix: String(config.s3Prefix).replace(/\/$/, ''),
		};
		return Promise.resolve(client._stagingLocation);
	}

	const catalog = config.catalog;
	const schema = config.schema;
	return new Promise((resolve, reject) => {
		const sql = `DESCRIBE SCHEMA EXTENDED \`${catalog}\`.\`${schema}\``;
		client.query(sql, [], (err, rows) => {
			if (err) return reject(err);
			const rootRow = (rows || []).find(r => r.database_description_item === 'RootLocation');
			if (!rootRow || !rootRow.database_description_value) {
				return reject(new Error(
					`Staging location unresolved: no config.s3Bucket/s3Prefix and no RootLocation row for ${catalog}.${schema}. ` +
					`Provide explicit s3Bucket+s3Prefix in dbconfig, or configure a managed location on the schema.`
				));
			}
			const rootUrl = rootRow.database_description_value.trim();
			const match = rootUrl.match(/^s3:\/\/([^/]+)\/(.+)$/);
			if (!match) {
				return reject(new Error(`Unexpected RootLocation format: ${rootUrl}`));
			}
			client._stagingLocation = { s3Bucket: match[1], s3Prefix: match[2].replace(/\/$/, '') };
			resolve(client._stagingLocation);
		});
	});
}

// ── streamToTableFromS3 implementation ───────────────────────────────────────
// Caller must supply `columns`, `s3Bucket`, `s3Prefix` in opts so setup is fully
// synchronous — no async describeTable / staging-location lookup happens here.
// Doing the lookup inside the pipeline race conditions with incoming rows
// (records arrive before the SDK promise resolves, no place to safely buffer).
function _streamToTableFromS3(client, table, opts) {
	const columnDefs = opts.columnDefs;
	const s3Bucket = opts.s3Bucket;
	const s3Prefix = opts.s3Prefix;
	if (!columnDefs || !columnDefs.length) throw new Error('streamToTableFromS3: columnDefs required ([{name, type}, ...])');
	if (!s3Bucket || !s3Prefix) throw new Error('streamToTableFromS3: s3Bucket and s3Prefix required');
	const columns = columnDefs.map(c => c.name);

	const cleanAuditDate = (opts.auditdate || client.auditdate || "'" + naiveIsoNow() + "'")
		.replace(/'/g, '').replace(/:/g, '-');
	const s3Key = `${s3Prefix}/${table}/${cleanAuditDate}.csv`;
	const s3Uri = `s3://${s3Bucket}/${s3Key}`;

	client._lastStagingS3 = { bucket: s3Bucket, key: s3Key };
	client._lastStagingS3Uri = s3Uri;
	client._lastStagingColumnDefs = columnDefs.slice();

	// Pre-compute which columns target TIMESTAMP_NTZ so the transform can strip
	// offset markers from incoming values. read_files in PERMISSIVE mode nulls any
	// timestamp value with a trailing `Z` or `±HH:MM` against an NTZ schema; the
	// audit columns were already handled at setAuditdate time, but payload columns
	// flow through this transform untouched. See ../CLAUDE.md "Timestamp handling".
	const ntzColumns = new Set(columnDefs.filter(c => isNtzType(c.type)).map(c => c.name));

	function nonNull(v) {
		if (v === '' || v === null || v === undefined) return '\\N';
		if (typeof v === 'string' && v.search(/\r/) !== -1) return v.replace(/\r\n?/g, '\n');
		return v;
	}

	let count = 0;

	// fast-csv pipe-delimited rows → leo-sdk's toS3 (the canonical Leo S3-write helper).
	// toS3's internal s3 client uses the AWS default credential chain at call time —
	// in production that's the Lambda IAM role; in local dev tests it's the env vars
	// set by the integration helper after assume-role.
	return ls.pipeline(
		csv.createWriteStream({
			headers: false,
			delimiter: '|',
			transform: (row, done) => done(null, columns.map(f => {
				const v = row[f];
				return nonNull(ntzColumns.has(f) ? stripTimestampOffset(v) : v);
			})),
		}),
		ls.through((row, done) => {
			count++;
			if (count % 10000 === 0) logger.info(table + ': ' + count);
			done(null, row);
		}),
		ls.toS3(s3Bucket, s3Key)
	);
}

// ── Timestamp normalization for TIMESTAMP_NTZ staging ────────────────────────
// read_files in PERMISSIVE mode nulls any timestamp value with a trailing `Z`
// or `±HH:MM` offset against a TIMESTAMP_NTZ schema. Producers commonly emit
// `Date.toISOString()` (always `Z`-suffixed) into timestamp payload fields —
// e.g. item-dw's `f_item_change_event.last_at`. Strip the offset suffix so the
// value reaches Delta as the same wall-clock Redshift's COPY TIMEFORMAT 'auto'
// would have stored. The connector inherits Redshift's deliberate no-TZ posture
// (see CLAUDE.md "Timestamp handling — preserving legacy no-TZ semantics");
// semantic normalization is a downstream concern.
//
// Matches ISO-8601-like `YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH[:?]MM]`. Values that
// don't match the shape (plain dates, non-strings, already naked) pass through.
const TS_OFFSET_RE = /^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)(Z|[+-]\d{2}:?\d{2})$/;
function stripTimestampOffset(value) {
	if (typeof value !== 'string') return value;
	const m = value.match(TS_OFFSET_RE);
	return m ? m[1] : value;
}

function isNtzType(type) {
	if (!type) return false;
	const t = String(type).toUpperCase().trim();
	return t === 'TIMESTAMP_NTZ';
}

// Exposed for unit tests; not part of the public client surface.
module.exports._stripTimestampOffset = stripTimestampOffset;
module.exports._isNtzType = isNtzType;

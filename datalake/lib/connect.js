'use strict';

const { DBSQLClient } = require('@databricks/sql');
const genericPool = require('generic-pool');
const logger = require('leo-logger')('connector.sql.datalake');
const csv = require('fast-csv');
// leo-sdk.streams is the canonical Leo stream module across LeoPlatform connectors
// (cf. connectors/postgres/lib/dwconnect.js — same pattern). It exposes pipeline,
// through, pipe, AND toS3 — use it for all stream primitives. The separate
// `leo-streams` npm package is a lighter sibling without toS3; we don't import it.
const ls = require('leo-sdk').streams;
const naiveIsoNow = require('./audit_timestamp.js');
const crypto = require('crypto');

// ── Config helpers ────────────────────────────────────────────────────────────
function clamp(val, min, max, defaultVal) {
	const n = (val != null && val !== '') ? Number(val) : defaultVal;
	if (isNaN(n)) return defaultVal;
	return Math.min(max, Math.max(min, n));
}

// ── Error classification ──────────────────────────────────────────────────────
// Connection-class errors (network, socket, session-closed) → destroy the pooled
// session and retry; query-class errors → release the session healthy.
// Conservative default: treat unknown errors as connection-class so MIN/MERGE
// (idempotent) get a retry rather than a poisoned session being returned to the pool.
// Known SQL-error patterns map to query-class to avoid spurious session churn.
function isConnectionError(err) {
	if (!err) return false;
	// Explicit network / transport error types from node-fetch and Node.js
	if (err.name === 'FetchError') return true;
	const code = err.code || '';
	if (['ECONNRESET', 'ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT'].includes(code)) return true;
	const msg = String(err.message || '').toLowerCase();
	if (msg.includes('socket hang up')) return true;
	if (msg.includes('session is closed') || msg.includes('session closed')) return true;
	if (msg.includes('transport error') || msg.includes('connection closed')) return true;
	// Known SQL compilation / permission error shapes → query-class (return false)
	if (msg.includes('sql compilation error') || msg.includes('parse_syntax_error') ||
		msg.includes('permission_denied') || msg.includes('table_or_view_not_found') ||
		msg.includes('schema_not_found') || msg.includes('syntax error')) return false;
	// Conservative default: treat remaining unknown errors as connection-class
	return true;
}

module.exports = function(config) {
	// ── Config normalization ───────────────────────────────────────────────────
	// poolMax: tuning knob (not a safety gate) — aligned to load.js parallelLimit(10).
	const poolMax = clamp(config.poolMax, 1, 50, 10);
	// statementTimeoutSeconds: runaway guard via STATEMENT_TIMEOUT session param.
	// Floored to 5 (not optional, prevents disabling via 0/negative), capped at 1800.
	// queryTimeout is ineffective on SQL Warehouses (per @databricks/sql contracts/).
	const statementTimeoutSeconds = clamp(config.statementTimeoutSeconds, 5, 1800, 600);
	// acquireTimeoutMillis: rejects a stuck acquire rather than hanging indefinitely.
	const acquireTimeoutMillis = (config.acquireTimeoutMillis != null)
		? Number(config.acquireTimeoutMillis) : 30000;
	// socketTimeout pinned above the STATEMENT_TIMEOUT cap so a long statement hits
	// the clean STATEMENT_TIMEOUT abort, not an unretried socket severance.
	// SDK default (900s) < 1800s cap, so we must raise it when cap is in force.
	const socketTimeoutMs = (statementTimeoutSeconds + 120) * 1000;
	// drainTimeoutMs: end()/disconnect() races pool.drain() against this deadline.
	// Prevents indefinitely-hung drain when a borrow never returns (stuck query or
	// Lambda freeze mid-flight). Abandoned borrows are cleaned up by pool.clear().
	const drainTimeoutMs = (config.drainTimeoutMs != null)
		? Number(config.drainTimeoutMs) : 5000;

	// ── Shared DBSQLClient + memoized connect ─────────────────────────────────
	// Constructed synchronously at factory time (no network) — unit tests that only
	// exercise pure helpers never trigger a connect(). Auth branch evaluated once.
	const sqlClient = new DBSQLClient();
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

	// Single shared promise — the first pool.acquire() burst fires N factory.create()
	// calls in parallel; without this guard they'd all call sqlClient.connect() and
	// each fetch a fresh OAuth token + open a new socket, defeating the refactor.
	let connectPromise = null;
	function ensureConnected() {
		if (!connectPromise) {
			// telemetryEnabled:false is merged into this.config inside connect() via
			// copyDefinedTelemetryOptions() — it must go here, not to new DBSQLClient().
			// Suppresses the per-host TelemetryClient warning that fires when multiple
			// DBSQLClient instances register for the same host (identity check, not value).
			connectPromise = sqlClient.connect(
				Object.assign({ socketTimeout: socketTimeoutMs, telemetryEnabled: false }, connOpts)
			);
		}
		return connectPromise;
	}

	// ── Schema cache ──────────────────────────────────────────────────────────
	let cache = {
		schema: {},
		timestamp: null,
	};

	// ── Pool concurrency observability ────────────────────────────────────────
	let peakBorrowed = 0;
	let peakPending = 0;

	// ── Pool factory ──────────────────────────────────────────────────────────
	const poolFactory = {
		create: async () => {
			await ensureConnected();
			const session = await sqlClient.openSession({
				initialCatalog: config.catalog,
				initialSchema: config.schema,
				// Session params: same as before + STATEMENT_TIMEOUT runaway guard.
				// ansi_mode=false: mirrors Redshift lenient cast/arithmetic semantics.
				// timezone=UTC + infer_timestamp_ntz_type=true: preserves NTZ wall-clocks.
				// STATEMENT_TIMEOUT (seconds, 0=disabled — we always set a positive value).
				initialParameters: {
					ansi_mode: 'false',
					infer_timestamp_ntz_type: 'true',
					timezone: 'UTC',
					STATEMENT_TIMEOUT: String(statementTimeoutSeconds),
				},
			});
			const wrapper = createSessionClient(session, cache, config);
			wrapper.dead = false;
			return wrapper;
		},
		destroy: async (wrapper) => {
			try { await wrapper._session.close(); } catch (e) { /* ignore */ }
		},
		validate: (wrapper) => Promise.resolve(wrapper.dead !== true),
	};

	const pool = genericPool.createPool(poolFactory, {
		max: poolMax,
		min: 0,
		testOnBorrow: true,
		acquireTimeoutMillis: acquireTimeoutMillis,
		autostart: false,
	});

	async function drainPool() {
		logger.info('Pool draining — peak stats', { peakBorrowed, peakPending, poolMax });
		// Correct shutdown sequence for generic-pool 3.x:
		// 1. pool.drain() — sets _draining=true; future releases auto-destroy instead of
		//    returning to idle. Returns a promise that resolves when _count===0.
		// 2. pool.clear() — destroys all idle resources, decrementing _count. This
		//    unblocks the drain promise: if no borrows remain, drain resolves here.
		// 3. Race the drain promise against a deadline so a stuck borrow (hung query or
		//    Lambda freeze) doesn't block shutdown indefinitely. Abandoned borrows are
		//    orphaned; the Lambda process exits and callbackWaitsForEmptyEventLoop=false.
		// Note: awaiting drain() before clear() deadlocks — drain waits for count===0,
		// but count only reaches 0 after clear destroys idle resources. Always start
		// drain (non-awaited) first to flip the flag, then clear, then await drain.
		const drainPromise = pool.drain();
		await pool.clear();
		const drainDeadline = new Promise(r => setTimeout(r, drainTimeoutMs));
		await Promise.race([drainPromise, drainDeadline]);
		connectPromise = null;
		try { await sqlClient.close(); } catch (e) { /* ignore */ }
	}

	const client = {
		// ── Schema cache ──────────────────────────────────────────────────────
		clearSchemaCache: () => {
			logger.info('Clearing Tables schema cache');
			cache.schema = {};
		},
		getSchemaCache: () => cache.schema,
		setSchemaCache: (s) => {
			cache.schema = s;
			cache.timestamp = Date.now();
		},

		// ── Connection ────────────────────────────────────────────────────────
		// connect() acquires a pooled session wrapper and returns it. Callers that
		// need a raw session (e.g. the existing unit tests) can call this; the
		// wrapper's release() returns it to the pool (no-op on the session itself).
		connect: async (_opts) => {
			await ensureConnected();
			return pool.acquire();
		},

		disconnect: async () => drainPool(),
		end: async () => drainPool(),
		release: () => {},

		// ── Query ─────────────────────────────────────────────────────────────
		// Routes through pool.acquire → execute → classify error → pool.release/destroy.
		query: (sql, paramsOrCb, cbOrOpts, opts) => {
			let params, cb;
			if (typeof paramsOrCb === 'function') {
				cb = paramsOrCb;
				params = [];
				opts = cbOrOpts;
			} else {
				params = paramsOrCb || [];
				cb = cbOrOpts;
			}

			pool.acquire().then(wrapper => {
				const borrowed = pool.borrowed;
				if (borrowed > peakBorrowed) peakBorrowed = borrowed;
				const pending = pool.pending;
				if (pending > peakPending) peakPending = pending;

				wrapper.query(sql, params, (err, rows, fields) => {
					if (err && isConnectionError(err)) {
						wrapper.dead = true;
						pool.destroy(wrapper).catch(() => {});
					} else {
						pool.release(wrapper).catch(() => {});
					}
					cb(err, rows, fields);
				}, opts);
			}).catch(cb);
		},

		// ── Schema describe ───────────────────────────────────────────────────
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

		// ── Identifier quoting ────────────────────────────────────────────────
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

		// ── Staging-location resolution ──────────────────────────────────────
		// Resolves the S3 staging bucket/prefix and stores them on the client
		// as `client.s3Bucket` / `client.s3Prefix`. Idempotent — call from the
		// bot at startup, or rely on importFact to await it before staging.
		// See ensureStagingLocation() at module bottom for the resolution rules.
		ensureStagingLocation: () => ensureStagingLocation(client, config),

		// Compute a deterministic, per-call S3 staging path. Mirrors postgres'
		// `qualifiedStagingTable` pattern (../postgres/lib/dwconnect.js:128-130):
		// the caller — importFact, the integration test — owns the staging
		// identifier and passes it down to streamToTableFromS3, so the staging
		// identity flows in one direction and never has to be communicated back
		// through shared state.
		stagingS3Path: (table, auditdate) => stagingS3Path(
			client.s3Bucket,
			client.s3Prefix,
			table,
			auditdate || client.auditdate
		),

		// ── S3 staging → inline read_files() ──────────────────────────────────
		// Stages CSV to S3. The downstream MERGE/MIN queries read it back via an
		// inline `read_files(...)` subquery rather than a CREATE TEMPORARY VIEW —
		// sessions are pooled and reused across queries, so a session-scoped temp
		// view from one acquire may not be visible to a later acquire.
		// See BUILD_PLAN.md Step 7 for the per-option rationale of the read_files arguments.
		streamToTableFromS3: (table, opts) => {
			return doStreamToTableFromS3(client, table, opts);
		},

		// Build the read_files() SELECT for use inline in MIN / MERGE queries.
		// columnDefs is `[{name, type}, ...]` — types must match the target table so
		// MERGE's COALESCE(staging.x, target.x) sees consistent types on both sides.
		//
		// _rescued_data is excluded from the positional schema: Databricks treats
		// rescuedDataColumn as a non-positional catch-all appended to the result.
		// Including it in the schema string displaces the columns that follow it
		// (they read as extra/unrecognized and land in _rescued_data as "_cN" keys).
		// Databricks appends _rescued_data automatically via rescuedDataColumn; the
		// staging SELECT * still exposes it for the MERGE UPDATE/INSERT clauses.
		buildStagingSelect: (s3Uri, columnDefs) => {
			const stagingCols = columnDefs.filter(c => c.name !== '_rescued_data');
			const schemaStr = stagingCols.map(c => `${c.name} ${c.type}`).join(', ');
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
				`  rescuedDataColumn => '_rescued_data',`,
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
// Wraps a single DBSQLSession. In the pool model:
//   - _session is exposed so the pool factory.destroy() can close it.
//   - dead flag is set by query() error classification; pool.validate() checks it.
//   - release() is a no-op — the pool owns lifecycle (acquire/release/destroy).
function createSessionClient(session, _parentCache, _config) {
	const conn = {
		_session: session,
		dead: false,

		query: async (sql, paramsOrCb, cbOrOpts, opts) => {
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
				const rawResult = await operation.fetchAll();
				await operation.close();
				const fields = rawResult.length ? Object.keys(rawResult[0]).map(name => ({ name })) : [];
				// inRowMode: leo-connector-common's mapResults (common/dol.js:492) requires
				// positional array rows so it can call r.slice(start, end) to extract
				// sub-object sections demarcated by `prefix_` sentinel columns. In postgres
				// this is free — pg's rowMode:'array' never builds objects in the driver.
				// Here, @databricks/sql fetchAll() always returns object rows, so we do an
				// extra object→array conversion just to satisfy the common contract.
				// The round-trip is: Databricks objects → arrays (here) → mapResults → objects.
				// Field order comes from Object.keys(rawResult[0]), which the Databricks SQL
				// driver preserves in SELECT column order — that ordering must match what
				// mapResults reads from the `fields` array.
				const result = (opts && opts.inRowMode && rawResult.length)
					? rawResult.map(row => fields.map(f => row[f.name]))
					: rawResult;
				cb(null, result, fields);
			} catch (err) {
				cb(err);
			}
		},

		release: () => {},
	};

	return conn;
}

// ── Staging location resolution ───────────────────────────────────────────────
// Single source of truth for client.s3Bucket / client.s3Prefix. Resolves once
// per client (idempotent re-entry returns the cached values) and pins the
// result on the client object. Downstream callers — streamToTableFromS3,
// importFact, bot wrappers — read client.s3Bucket / client.s3Prefix directly
// rather than re-invoking the resolver.
//
// Preference order:
//   1. config.s3Bucket + config.s3Prefix (set by the bot / test helper)
//   2. UC RootLocation lookup via DESCRIBE SCHEMA EXTENDED (fallback for
//      schemas configured with a managed storage location)
//
// The UC fallback uses the `RootLocation` row, not `Location` (UUID-suffixed
// internal path) or the catalog `Storage Root` (managed-table area, not a
// staging convention).
function ensureStagingLocation(client, config) {
	if (client.s3Bucket && client.s3Prefix) {
		return Promise.resolve({ s3Bucket: client.s3Bucket, s3Prefix: client.s3Prefix });
	}

	if (config.s3Bucket && config.s3Prefix) {
		client.s3Bucket = config.s3Bucket;
		client.s3Prefix = String(config.s3Prefix).replace(/\/$/, '');
		return Promise.resolve({ s3Bucket: client.s3Bucket, s3Prefix: client.s3Prefix });
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
			client.s3Bucket = match[1];
			client.s3Prefix = match[2].replace(/\/$/, '');
			resolve({ s3Bucket: client.s3Bucket, s3Prefix: client.s3Prefix });
		});
	});
}

// ── stagingS3Path implementation ─────────────────────────────────────────────
// Pure: compute the per-call staging S3 path from (bucket, prefix, table,
// auditdate). The caller owns the resulting identifier — same shape as
// postgres' `qualifiedStagingTable`, just with an S3 file as the staging
// artifact instead of a temp table. The random suffix ensures uniqueness
// across concurrent same-table imports.
function stagingS3Path(s3Bucket, s3Prefix, table, auditdate) {
	if (!s3Bucket || !s3Prefix) {
		throw new Error('stagingS3Path: s3Bucket/s3Prefix unresolved — call client.ensureStagingLocation() first');
	}
	const cleanAuditDate = String(auditdate || "'" + naiveIsoNow() + "'")
		.replace(/'/g, '').replace(/:/g, '-');
	const uniqueSuffix = crypto.randomBytes(4).toString('hex');
	const key = `${String(s3Prefix).replace(/\/$/, '')}/${table}/${cleanAuditDate}-${uniqueSuffix}.csv`;
	return {
		bucket: s3Bucket,
		key,
		uri: `s3://${s3Bucket}/${key}`,
	};
}

// ── streamToTableFromS3 implementation ───────────────────────────────────────
// Caller supplies `columnDefs` and either an explicit `s3Path` ({bucket, key,
// uri}) — produced via client.stagingS3Path(table, auditdate) — or
// (s3Bucket/s3Prefix, auditdate) for the call to derive the path internally
// for backwards compatibility with callers that don't yet own the identifier.
// Setup is fully synchronous — no async describeTable / staging-location lookup
// happens here. Doing the lookup inside the pipeline race conditions with
// incoming rows (records arrive before the SDK promise resolves, no place to
// safely buffer).
function doStreamToTableFromS3(client, table, opts) {
	opts = opts || {};
	const columnDefs = opts.columnDefs;
	if (!columnDefs || !columnDefs.length) throw new Error('streamToTableFromS3: columnDefs required ([{name, type}, ...])');

	let s3Bucket, s3Key;
	if (opts.s3Path) {
		s3Bucket = opts.s3Path.bucket;
		s3Key = opts.s3Path.key;
	} else {
		s3Bucket = opts.s3Bucket || client.s3Bucket;
		const s3Prefix = opts.s3Prefix || client.s3Prefix;
		if (!s3Bucket || !s3Prefix) {
			throw new Error('streamToTableFromS3: s3Bucket/s3Prefix unresolved — call client.ensureStagingLocation() before staging, or pass them in opts');
		}
		const path = stagingS3Path(s3Bucket, s3Prefix, table, opts.auditdate || client.auditdate);
		s3Key = path.key;
	}
	// Exclude _rescued_data from positional CSV columns — same reason as buildStagingSelect.
	const columns = columnDefs.filter(c => c.name !== '_rescued_data').map(c => c.name);

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
	// Native Date objects are inherently UTC instants — producers that hand a
	// Date in (e.g. via JSON revivers or direct assignment) would otherwise
	// bypass the strip and reach the CSV as `.toISOString()` output with the
	// Z intact, which read_files under TIMESTAMP_NTZ then nulls. Coerce to
	// the ISO form so the regex below picks up the Z.
	if (value instanceof Date) {
		if (isNaN(value.getTime())) return value;
		value = value.toISOString();
	}
	if (typeof value !== 'string') return value;
	const m = value.match(TS_OFFSET_RE);
	return m ? m[1] : value;
}

function isNtzType(type) {
	if (!type) return false;
	const t = String(type).toUpperCase().trim();
	return t === 'TIMESTAMP_NTZ';
}

// Exposed alongside the factory for unit tests and bot use.
module.exports.stripTimestampOffset = stripTimestampOffset;
module.exports.isNtzType = isNtzType;
module.exports.stagingS3Path = stagingS3Path;
module.exports.isConnectionError = isConnectionError;

'use strict';

const { DBSQLClient } = require('@databricks/sql');
const logger = require('leo-logger')('connector.sql.datalake');
const csv = require('fast-csv');
const ls = require('leo-streams');

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
			await dbsql.connect({
				host: config.host,
				path: config.path,
				token: config.token,
			});

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
				const sql = `SELECT table_name, column_name, data_type, is_nullable FROM ${catalog}.information_schema.columns WHERE table_schema = ? ORDER BY ordinal_position ASC`;
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

		// ── S3 staging → temp view ────────────────────────────────────────
		// See BUILD_PLAN.md Step 7 for the full rationale on each read_files option.
		streamToTableFromS3: (table, config) => {
			return _streamToTableFromS3(client, table, config);
		},

		// streamToTable: direct-write path (not used in Databricks; kept for interface parity)
		streamToTable: () => {
			throw new Error('streamToTable is not implemented for Databricks; use streamToTableFromS3');
		},
	};

	function setAuditdate() {
		client.auditdate = "'" + new Date().toISOString().replace(/\.\d*Z/, 'Z') + "'";
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
				const operation = await session.executeStatement(sql, {
					parameters: params.map((v, i) => ({ name: String(i), value: v })),
					runAsync: false,
				});
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

// ── RootLocation lookup ───────────────────────────────────────────────────────
// Queries DESCRIBE SCHEMA EXTENDED and parses the RootLocation row, which is the
// managed storage root configured in Unity Catalog and doubles as the S3 staging
// prefix for this schema. Result is cached on the client; do not use the Location
// row (internal UC path with UUID) or the catalog's Storage Root (different convention).
function _ensureStagingLocation(client, catalog, schema) {
	if (client._stagingLocation) return Promise.resolve(client._stagingLocation);

	return new Promise((resolve, reject) => {
		const sql = `DESCRIBE SCHEMA EXTENDED \`${catalog}\`.\`${schema}\``;
		client.query(sql, [], (err, rows) => {
			if (err) return reject(err);
			const rootRow = (rows || []).find(r => r.database_description_item === 'RootLocation');
			if (!rootRow || !rootRow.database_description_value) {
				return reject(new Error(
					`RootLocation not found for ${catalog}.${schema} — schema must have a managed storage location configured in Unity Catalog`
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
function _streamToTableFromS3(client, table, config) {
	let columns = [];
	let s3Stream = null;
	let s3Key = null;
	let s3Uri = null;
	let pending = null;
	let schemaReady = false;

	// Resolve schema columns and S3 staging location from Unity Catalog in parallel.
	Promise.all([
		client.describeTable(table.replace(/^.*\./, ''), config.schema),
		_ensureStagingLocation(client, config.catalog, config.schema),
	]).then(([tableFields, stagingLocation]) => {
		columns = tableFields.map(f => f.column_name);
		schemaReady = true;

		const cleanAuditDate = client.auditdate.replace(/'/g, '').replace(/:/g, '-');
		s3Key = `${stagingLocation.s3Prefix}/${table}/${cleanAuditDate}.csv`;
		s3Uri = `s3://${stagingLocation.s3Bucket}/${s3Key}`;

		const awsS3 = new (require('aws-sdk')).S3({ region: config.region });
		s3Stream = ls.toS3(stagingLocation.s3Bucket, s3Key, { s3: awsS3 });
		client._lastStagingS3 = { s3: awsS3, bucket: stagingLocation.s3Bucket, key: s3Key };
		s3Stream.on('finish', () => s3Stream.emit('end'));
		s3Stream.on('error', err => {
			logger.error('S3 stream error:', err);
		});

		if (pending) pending();
	}).catch(err => {
		logger.error('setup error in streamToTableFromS3:', err);
	});

	// Null/newline normalization matching connectors/postgres/lib/connect.js:394-402
	function nonNull(v) {
		if (v === '' || v === null || v === undefined) return '\\N';
		if (typeof v === 'string' && v.search(/\r/) !== -1) return v.replace(/\r\n?/g, '\n');
		return v;
	}

	let count = 0;

	return ls.pipeline(
		csv.createWriteStream({
			headers: false,
			delimiter: '|',
			transform: (row, done) => {
				if (!schemaReady) {
					pending = () => done(null, columns.map(f => nonNull(row[f])));
				} else {
					done(null, columns.map(f => nonNull(row[f])));
				}
			},
		}),
		ls.write(
			(r, done) => {
				count++;
				if (count % 10000 === 0) logger.info(table + ': ' + count);
				if (!s3Stream.write(r)) {
					s3Stream.once('drain', done);
				} else {
					done(null);
				}
			},
			(done) => {
				if (s3Stream) {
					s3Stream.on('end', async (err) => {
						if (err) return done(err);

						// Mount staged file as a temporary view, then signal done.
						// The MERGE itself is issued by dwconnect.importFact after streamToTableFromS3 completes.
						const viewName = `staging_${table.replace(/[^a-z0-9_]/gi, '_')}`;
						const colDefs = columns.map(c => `${c} STRING`).join(', ');

						// read_files options — see BUILD_PLAN.md Step 7 for per-option rationale
						const viewSql = [
							`CREATE OR REPLACE TEMPORARY VIEW \`${viewName}\` AS`,
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
							`  schema => '${colDefs}'`,
							`)`,
						].join('\n');

						client.query(viewSql, [], (viewErr) => {
							if (viewErr) return done(viewErr);
							client._lastStagingView = viewName;
							done();
						});
					});
					s3Stream.end();
				} else {
					done();
				}
			}
		)
	);
}

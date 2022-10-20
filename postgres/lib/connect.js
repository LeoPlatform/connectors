const {
	Pool,
} = require('pg');
const logger = require('leo-logger')('connector.sql.postgres');
const moment = require('moment');
const format = require('pg-format');
const async = require('async');
const sqlstring = require('sqlstring');

var copyFrom = require('pg-copy-streams').from;
var copyTo = require('pg-copy-streams').to;
let csv = require('fast-csv');

// need to confirm there is no issue adding these dependencies
const leo = require('leo-sdk');
const ls = leo.streams;

require('pg').types.setTypeParser(1114, (val) => {
	val += 'Z';
	logger.debug(val);
	return moment(val).unix() + '  ' + moment(val).utc().format();
});

let queryCount = 0;
module.exports = function (config) {
	const pool = new Pool(Object.assign({
		database: 'test',
		host: 'localhost',
		password: 'a',
		port: 5432,
		user: 'root',
	}, config, {
		max: 15,
	}));

	return create(pool);
};

function create (pool, parentCache) {
	let cache = {
		schema: Object.assign({}, (parentCache && parentCache.schema) || {}),
		timestamp: (parentCache && parentCache.timestamp) || null,
	};
	let client = {
		clearSchemaCache: () => {
			logger.info(`Clearing Tables schema cache`);
			cache.schema = {};
		},
		connect: (opts) => {
			opts = opts || {};
			return pool.connect().then(c => {
				return create(c, opts.type === 'isolated' ? {} : cache);
			});
		},
		describeTable: async (table, tableSchema = 'public') => {
			return new Promise((resolve, reject) => {
				const qualifiedTable = `${tableSchema}.${table}`;
				if (cache.schema[qualifiedTable]) {
					logger.info(`Table "${qualifiedTable}" schema from cache`, cache.timestamp);
					resolve(cache.schema[qualifiedTable]);
				} else {
					client.describeTables(tableSchema).then(schema => {
						if (schema && schema[qualifiedTable]) {
							return resolve(schema[qualifiedTable]);
						}

						reject('NO_SCHEMA_FOUND');
					});
				}
			});
		},
		describeTables: async (tableSchema = 'public') => {
			return new Promise((resolve, reject) => {
				if (Object.keys(cache.schema || {}).length) {
					logger.info(`Tables schema from cache`, cache.timestamp);
					resolve(cache.schema);
				}

				client.query(`SELECT table_name, column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = '${tableSchema}' order by ordinal_position asc`, (err, result) => {
					if (err) {
						throw err;
					}

					let schema = {};
					result && result.map(r => {
						const qualifiedTable = `${tableSchema}.${r.table_name}`;
						if (!(qualifiedTable in schema)) {
							schema[qualifiedTable] = [];
						}
						schema[qualifiedTable].push(r);
					});
					Object.keys(schema).map((t) => {
						let parts = t.match(/^public\.(.*)$/);
						if (parts) {
							schema[parts[1]] = schema[t];
						}
					});
					cache.schema = schema;
					cache.timestamp = Date.now();
					logger.info('Caching Schema Table', cache.timestamp);
					resolve(cache.schema);
				});
			});
		},
		disconnect: pool.end.bind(pool),
		end: pool.end.bind(pool),
		escape: (value) => {
			if (value.replace) {
				return '"' + value.replace('"', '') + '"';
			} else {
				return value;
			}
		},
		escapeId: (field) => {
			return '"' + field.replace('"', '').replace(/\.([^.]+)$/, '"."$1') + '"';
		},
		escapeValue: (value) => {
			if (value.replace) {
				return '\'' + value.replace('\'', '\\\'').toLowerCase() + '\'';
			} else {
				return value;
			}
		},
		escapeValueNoToLower: (value) => {
			if (value.replace) {
				return '\'' + value.replace('\'', '\\\'') + '\'';
			} else {
				return value;
			}
		},
		getIds: (table, id, start, end, reverse, callback) => {
			let sql;

			if (Array.isArray(id)) {
				let direction = (reverse) ? 'DESC' : 'ASC';
				let startDirection = (reverse) ? '<=' : '>=';
				let endDirection = (reverse) ? '>=' : '<=';
				let where = [];
				let order = [];

				for (let key of id) {
					let startValue = sqlstring.escape(start[key]);
					let endValue = sqlstring.escape(end[key]);

					where.push(`(${key} ${startDirection} ${startValue} AND ${key} ${endDirection} ${endValue})`);

					order.push(`${key} ${direction}`);
				}

				sql = `SELECT ${id.join(',')}
					FROM ${table}
					WHERE ${where.join(' AND ')}
					ORDER BY ${order.join(',')}`;
			} else {
				if (reverse) {
					sql = `select ${id} as id from ${table}  
						where ${id} <= ${start} and ${id} >= ${end}
						ORDER BY ${id} desc`;
				} else {
					sql = `select ${id} as id from ${table}  
						where ${id} >= ${start} and ${id} <= ${end}
						ORDER BY ${id} asc`;
				}
			}

			client.query(sql, callback);
		},
		getSchemaCache: () => {
			return cache.schema || {};
		},
		nibble: (table, id, start, min, max, limit, reverse, callback) => {
			let sql;

			// handle composite keys
			if (Array.isArray(id)) {
				let where = [];
				let orderDirection = (reverse) ? 'DESC' : 'ASC';
				let startDirection = (reverse) ? '<=' : '>=';
				let endDirection = (reverse) ? '>=' : '<=';
				let values = (reverse) ? min : max;

				// build the where clause
				for (let key in values) {
					where.push(`(${key} ${startDirection} ${start[key]} AND ${key} ${endDirection} ${values[key]})`);
				}

				// build the order
				let order = id.join(` ${orderDirection},`) + ` ${orderDirection}`;

				sql = `SELECT ${id.join(', ')}
					FROM ${table}
					WHERE ${where.join(' AND ')}
					ORDER BY ${order}`;
			} else {
				if (reverse) {
					sql = `select ${id} as id from ${table}  
							where ${id} <= ${start} and ${id} >= ${min}
							ORDER BY ${id} desc`;
				} else {
					sql = `select ${id} as id from ${table}  
							where ${id} >= ${start} and ${id} <= ${max}
							ORDER BY ${id} asc`;
				}
			}

			sql += ` LIMIT 2 OFFSET ${limit - 1}`;
			client.query(sql, callback);
		},
		query: (query, params, callback, opts = {}) => {
			if (typeof params === 'function') {
				opts = callback;
				callback = params;
				params = [];
			}
			opts = Object.assign({
				inRowMode: false,
				stream: false,
			}, opts || {});
			let queryId = ++queryCount;
			let log = logger.sub('query');
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			pool.query({
				rowMode: opts.inRowMode ? 'array' : undefined,
				text: query,
				values: params,
			}, function (err, result) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					if (!opts.allowError) {
						log.error(`Had error #${queryId}`, err, query);
					}
					callback(err);
				} else {
					if (opts && opts.returnResultObject) {
						callback(null, result);
					} else {
						callback(null, result.rows, result.fields);
					}
				}
			});
		},
		range: (table, id, opts, callback) => {
			// handle composite keys
			if (Array.isArray(id)) {
				let SPLIT_KEY = '::';

				// get the first member of id as the key
				let key;
				for (key of id) break;

				let minKey = 'CONCAT(MIN(' + id.join(`), '${SPLIT_KEY}', MIN(`) + '))';
				let maxKey = 'CONCAT(MAX(' + id.join(`), '${SPLIT_KEY}', MAX(`) + '))';

				client.query(`SELECT ${minKey} AS min, ${maxKey} AS max, COUNT(${key}) AS total FROM ${table}`, (err, result) => {
					if (err) return callback(err);

					let results = {
						max: {},
						min: {},
						total: result[0].total,
					};
					let min = result[0].min.split(SPLIT_KEY);
					let max = result[0].max.split(SPLIT_KEY);

					for (let i in id) {
						results.min[id[i]] = min[i];
						results.max[id[i]] = max[i];
					}

					callback(null, results);
				});
			} else { // do things normally
				client.query(`select min(${id}) as min, max(${id}) as max, count(${id}) as total from ${table}`, (err, result) => {
					if (err) return callback(err);
					callback(null, {
						max: result[0].max,
						min: result[0].min,
						total: result[0].total,
					});
				});
			}
		},
		release: (destroy) => {
			pool.release && pool.release(destroy);
		},
		setAuditdate,
		setSchemaCache: (schema) => {
			cache.schema = schema || {};
		},
		streamFromTable: (table, opts) => {
			function clean (v) {
				let i = v.search(/(\r|\n)/);
				if (i !== -1 && i < v.length - 1) {
					// Escape any newlines in the middle of the row
					return v.substr(0, v.length - 1).replace(/\n/g, `\\n`) + v[v.length - 1];
				}
				return v;
			}

			opts = Object.assign({
				columns: null,
				delimiter: '|',
				encoding: 'utf-8',
				format: 'csv',
				headers: false,
				null: '\\N',
				where: null,
			}, opts);

			// let columns = [];
			let stream;
			let count = 0;
			let pass = ls.through((row, done) => {
				count++;
				if (count % 10000 === 0) {
					logger.info(table + ': ' + count);
				}
				done(null, clean(row.toString()));
			});
			let myClient = null;
			pool.connect().then(c => {
				// client.describeTable(table, (err, result) => {
				//	columns = result.map(f => f.column_name);
				myClient = c;
				let columnsList = '';
				if (Array.isArray(opts.columns)) {
					columnsList = opts.columns.join(',');
				} else if (typeof opts.columns === 'string') {
					columnsList = opts.columns;
				}
				let query = `COPY ${opts.where ? `(select ${columnsList || '*'} from ${table} ${opts.where})` : `${table} (${columnsList})`} TO STDOUT (format ${opts.format}, null '${opts.null}', encoding '${opts.encoding}', DELIMITER '${opts.delimiter}', HEADER ${opts.headers})`;

				logger.log(query);
				stream = myClient.query(copyTo(query));
				stream.on('error', function (err) {
					logger.error(err);
				});
				stream.on('end', function () {
					logger.info('Copy stream ended', table);
					myClient.release(true);
				});
				ls.pipe(stream, pass);
				// });
			}, err => {
				logger.error(err);
				pass.emit('error', err);
			});
			return pass;
		},
		streamToTable: (table) => {
			const ts = table.split('.');
			let schema = 'public';
			let shortTable = table;
			if (ts.length > 1) {
				schema = ts[0];
				shortTable = ts[1];
			}

			// opts = Object.assign({
			// 	records: 10000
			// }, opts || {});
			let columns = [];
			let stream;
			let myClient = null;
			let pending = null;
			let ended = false;
			pool.connect().then(c => {
				client.describeTable(shortTable, schema).then(result => {
					if (ended) {
						c.release(true);
						return;
					}
					columns = result.map(f => f.column_name);
					myClient = c;
					logger.log(`COPY ${table} FROM STDIN (format csv, null '\\N', encoding 'utf-8')`);
					stream = myClient.query(copyFrom(`COPY ${table} FROM STDIN (format csv, null '\\N', encoding 'utf-8')`));
					stream.on('error', function (err) {
						logger.error(`COPY error: ${err.where}`, err);
						process.exit();
					});
					if (pending) {
						pending();
					}
				}).catch(err => {
					logger.error(err);
				});
			}, err => {
				logger.error(err);
			});

			let count = 0;

			function nonNull (v) {
				if (v === '' || v === null || v === undefined) {
					return '\\N';
				} else if (typeof v === 'string' && v.search(/\r/) !== -1) {
					return v.replace(/\r\n?/g, '\n');
				} else {
					return v;
				}
			}

			return ls.pipeline(csv.createWriteStream({
				headers: false,
				transform: (row, done) => {
					if (!myClient) {
						pending = () => {
							done(null, columns.map(f => nonNull(row[f])));
						};
					} else {
						done(null, columns.map(f => nonNull(row[f])));
					}
				},
			}), ls.write((r, done) => {
				count++;
				if (count % 10000 === 0) {
					logger.info(table + ': ' + count);
				}
				if (!stream.write(r)) {
					stream.once('drain', done);
				} else {
					done(null);
				}
			}, (done) => {
				ended = true;
				if (stream) {
					stream.on('end', () => {
						myClient.release(true);
						done();
					});
					stream.end();
				} else {
					done();
				}
			}));
		},
		streamToTableBatch: (table, opts) => {
			opts = Object.assign({
				records: 10000,
				useOnDuplicateUpdate: false,
				useReplaceInto: false,
			}, opts || {});
			let pending = null;
			let columns = [];
			let ready = false;
			let total = 0;

			let primaryKey = [];
			let uniqueKeys = {};
			let uniqueLookup = {};

			client.query(`select c.column_name, pk.constraint_name, pk.constraint_type
					FROM information_schema.columns c
					LEFT JOIN (SELECT kc.column_name, kc.constraint_name, tc.constraint_type
  						FROM information_schema.key_column_usage kc
  						JOIN information_schema.table_constraints tc on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
  						WHERE tc.table_name = $1
    						and tc.table_schema = 'public'
						) pk on pk.column_name = c.column_name
					WHERE 
   					c.table_name = $1 and c.table_schema = 'public';
			`, [table], (err, results) => {
				columns = results.map(r => {
					if (r.constraint_type) {
						if (r.constraint_type.toLowerCase() === 'primary key') {
							primaryKey.push(r.column_name);
						}
						if (r.constraint_type.toLowerCase() === 'unique') {
							if (!(r.constraint_name in uniqueKeys)) {
								uniqueKeys[r.constraint_name] = [];
							}
							if (!(r.column_name in uniqueLookup)) {
								uniqueLookup[r.column_name] = [];
							}
							uniqueKeys[r.constraint_name].push(r.column_name);
							uniqueLookup[r.column_name].push(r.constraint_name);
						}
					}
					return r.column_name;
				}).filter((v, i, self) => {
					return self.indexOf(v) === i;
				});
				ready = true;
				if (pending) {
					pending();
				}
			});
			return ls.bufferBackoff((obj, done) => {
				if (!ready) {
					pending = () => {
						done(null, obj, 1, 1);
					};
				} else {
					done(null, obj, 1, 1);
				}
			}, (records, callback) => {
				if (opts.useReplaceInto) {
					logger.info('Replace Inserting ' + records.length + ' records of ', total);
				} else {
					logger.info('Inserting ' + records.length + ' records of ', total);
				}
				total += records.length;
				let primaryKeyLists = [];
				let toRemoveRecords = {};
				Object.keys(uniqueKeys).forEach(constraint => {
					toRemoveRecords[constraint] = [];
				});
				let values = records.map((r) => {
					Object.keys(uniqueKeys).forEach(constraint => {
						toRemoveRecords[constraint].push(
							uniqueKeys[constraint].map(f => r[f])
						);
					});
					if (opts.onInsert && primaryKey.length) {
						primaryKeyLists.push(primaryKey.map(f => r[f]));
					}
					return columns.map(f => r[f]);
				});
				let cmd = 'INSERT INTO %I (%I) VALUES %L';
				if (opts.useOnDuplicateUpdate && primaryKey.length) {
					cmd += ` ON CONFLICT (${primaryKey.join(',')}) DO UPDATE SET ` + columns.map(f => `
						${f} = EXCLUDED.${f}
					`);
				}
				let insertQuery = format(cmd, table, columns, values, columns);
				client.query(insertQuery, function (err) {
					if (err) {
						let tasks = [];
						tasks.push(done => client.query('BEGIN', done));
						tasks.push(done => client.query(format('ALTER TABLE %I DISABLE TRIGGER ALL', table), done));
						Object.keys(uniqueKeys).forEach(constraint => {
							tasks.push(done => {
								// Let's delete the offending records and try again
								client.query(format('DELETE FROM %I WHERE ROW(%I) in (%L)', table, uniqueKeys[constraint], toRemoveRecords[constraint]), done);
							});
						});
						async.series(tasks, err => {
							if (err) {
								callback(err);
							} else {
								client.query(insertQuery, function (err) {
									if (err) {
										callback(err);
									} else {
										client.query('COMMIT', err => {
											if (err) {
												logger.error(err);
												callback(err);
											} else {
												if (opts.onInsert) {
													opts.onInsert(primaryKeyLists);
												}
												callback(null, []);
											}
										});
									}
								});
							}
						});
					} else {
						if (opts.onInsert) {
							opts.onInsert(primaryKeyLists);
						}
						callback(null, []);
					}
				}, {
					// allowError: true
				});
			}, {
				failAfter: 2,
			}, {
				records: opts.records,
			});
		},
		streamToTableFromS3: (table, config) => {
			const ts = table.split('.');
			let schema = 'public';
			let shortTable = table;
			if (ts.length > 1) {
				schema = ts[0];
				shortTable = ts[1];
			}

			let columns = [];
			let stream;
			let myClient = null;
			let pending = null;
			let ended = false;
			let csvopts = { delimiter: '|' };
			let keepS3Files = config.keepS3Files != null ? config.keepS3Files : false;

			// Get prefix for S3 file path
			let s3prefix =
					config.s3prefix ||
					process.env.AWS_LAMBDA_FUNCTION_NAME ||
					'dw_redshift_ingest';
			s3prefix = s3prefix.replace(/^\/*(.*?)\/*$/, '$1'); // Remove leading and trailing '/'

			// clean audit date to use in S3 file path
			let cleanAuditDate = client.auditdate
				.replace(/'/g, '')
				.replace(/:/g, '-');
			let s3FileName = `files/${s3prefix}/${cleanAuditDate}/${table}.csv`;

			client.connect().then(
				c => {
					client
						.describeTable(shortTable, schema)
						.then(result => {
							if (ended) {
								c.release(true);
								return;
							}
							columns = result.map(f => f.column_name);
							myClient = c;

							stream = ls.toS3(leo.configuration.resources.LeoS3, s3FileName);
							// copyFrom uses `end` but s3 `finish` so pipe finish to end
							stream.on('finish', () => stream.emit('end'));

							stream.on('error', function(err) {
								logger.error(`COPY error: ${err.where}`, err);
								process.exit();
							});
							if (pending) {
								pending();
							}
						})
						.catch(err => {
							logger.error(err);
						});
				},
				err => {
					logger.error(err);
				}
			);

			let count = 0;

			function nonNull(v) {
				if (v === '' || v === null || v === undefined) {
					return '\\N';
				} else if (typeof v === 'string' && v.search(/\r/) !== -1) {
					return v.replace(/\r\n?/g, '\n');
				} else {
					return v;
				}
			}

			
			return ls.pipeline(
				csv.createWriteStream({
					...csvopts,
					headers: false,
					transform: (row, done) => {
						if (!myClient) {
							pending = () => {
								done(
									null,
									columns.map(f => nonNull(row[f]))
								);
							};
						} else {
							done(
								null,
								columns.map(f => nonNull(row[f]))
							);
						}
					},
				}),
				ls.write(
					(r, done) => {
						count++;
						if (count % 10000 === 0) {
							logger.info(table + ': ' + count);
						}
						if (!stream.write(r)) {
							stream.once('drain', done);
						} else {
							done(null);
						}
					},
					done => {
						ended = true;
						logger.debug(table + ': stream done');
						if (stream) {
							stream.on('end', err => {
								logger.debug(table + ': stream ended', err || '');

								// wrap done callback to release the connection
								function innerDone(err) {
									myClient.release(true);
									logger.debug(table + ': stream client released', err || '');
									done(err);
								}

								if (err) {
									innerDone(err);
								} else {
									// Once the S3 file is complete run copy to load the staging table
									let f = columns.map(f => `"${f}"`);
									let file = `s3://${leo.configuration.s3}/${s3FileName}`;
									let manifest = '';
									let role = config.loaderRole;
									myClient.query(
										`copy ${table} (${f}) from '${file}' ${manifest} ${role ? `credentials 'aws_iam_role=${role}'` : ''
										} NULL AS '\\\\N' format csv DELIMITER '|' ACCEPTINVCHARS TRUNCATECOLUMNS ACCEPTANYDATE TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' COMPUPDATE OFF`,
										copyErr => {
											if (keepS3Files) {
												innerDone(copyErr);
											} else {
												// Delete the S3 files when done
												ls.s3.deleteObject(
													{
														Bucket: leo.configuration.s3,
														Key: s3FileName,
													},
													deleteError => {
														if (deleteError) {
															logger.info(
																'file failed to delete:',
																s3FileName,
																deleteError
															);
														}
														innerDone(copyErr);
													}
												);
											}
										}
									);
								} 
							});
							stream.end();
						} else {
							done();
						}
					}
				)
			);
		},
	};

	function setAuditdate () {
		client.auditdate = '\'' + new Date().toISOString().replace(/\.\d*Z/, 'Z') + '\'';
	}

	return client;
}

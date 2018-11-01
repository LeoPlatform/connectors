"use strict";

const mysql = require("mysql2");
const logger = require("leo-logger")("leo.connector.sql.mysql");
let ls = require("leo-sdk").streams;
let connections = {};

module.exports = function(c) {
	let config = Object.assign({
		host: "localhost",
		user: c.username || 'root', // use username if it is passed through (secrets manager)
		port: 3306,
		database: c.dbname || "datawarehouse", // use dbname if it is passed through (secrets manager)
		password: "a",
		connectionLimit: 10,
		timezone: 'utc'
	}, c);

	// delete invalid options so we don't throw an error in future versions of mysql
	delete config.username;
	delete config.dbname;
	delete config.engine;
	delete config.table;
	delete config.id;
	delete config.version;
	delete config.type;

	let connectionHash = JSON.stringify(config);
	let pool;

	if (!(connectionHash in connections)) {
		console.log("CREATING NEW MYSQL CONNECTION");
		connections[connectionHash] = mysql.createPool(config);
	} else {
		console.log("REUSING CONNECTION");
	}
	pool = connections[connectionHash];
	
	let cache = {
		schema: {},
		timestamp: null
	};

	let queryCount = 0;
	let client = {
		setAuditdate,
		connect: function(opts) {
			opts = opts || {};
			return new Promise((resolve, reject) => {
				pool.getConnection((error, connection) => {
					if (error) {
						reject(error);
					}
					resolve(connection);
				});
			});
		},
		query: function(query, params, callback, opts = {}) {
			if (typeof params == "function") {
				opts = callback;
				callback = params;
				params = [];
			}
			opts = Object.assign({
				inRowMode: false,
				stream: false
			}, opts || {});

			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query, params && params.length ? params : "");
			log.time(`Ran Query #${queryId}`);

			pool.query({
				sql: query,
				rowsAsArray: opts.inRowMode
			}, params, function(err, result, dbfields) {
				log.timeEnd(`Ran Query #${queryId}`);
				let fields;
				if (err) {
					log.error(`Had error #${queryId}`, query, err);
				} else if (dbfields) {
					// make fields interchangeable between mysql and mysql2 node modules
					fields = dbfields.map(data => {
						let startingObj = {
							type: data.columnType,
							db: data.schema,
							length: data.columnLength,
							schema: ''
						};

						Object.keys(data).filter(f => !f.match(/^_/)).filter(f => data[f]).map(k => {
							startingObj[k] = data[k];
						});

						return startingObj;
					});
				}

				callback(err, result, fields);
			});
		},
		end: function(callback) {
			connections[connectionHash] = undefined;
			return pool.end(callback);
		},
		disconnect: function(callback) {
			return this.end(callback);
		},
		release: function (destroy) {
			pool.release && pool.release(destroy);
		},
		describeTable: function (table, callback, tableSchema = config.database) {
			const qualifiedTable = `${tableSchema}.${table}`;
			if (cache.schema[qualifiedTable]) {
				callback(null, cache.schema[qualifiedTable] || []);
			} else {
				this.clearSchemaCache();
				this.describeTables((err, schema) => {
					callback(err, schema && schema[qualifiedTable] || []);
				}, tableSchema);
			}
		},
		describeTables: function(callback, tableSchema = config.database) {
			if (Object.keys(cache.schema || {}).length) {
				logger.info(`Tables schema from cache`, cache.timestamp);
				return callback(null, cache.schema);
			}
			client.query(`SELECT table_name, column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = '${tableSchema}' order by ordinal_position asc`, (err, result) => {
				let schema = {};
				result && result.map(tableInfo => {
					const tableName = `${tableSchema}.${tableInfo.table_name}`;
					if (!schema[tableName]) {
						schema[tableName] = [];
					}
					schema[tableName].push(tableInfo);
				});
				Object.keys(schema).map((key) => {
					let parts = key.match(/^datawarehouse\.(.*)$/);
					if (parts) {
						schema[parts[1]] = schema[key];
					}
				});
				cache.schema = schema;
				cache.timestamp = Date.now();
				logger.info("Caching Schema Table", cache.timestamp);
				callback(err, cache.schema);
			});
		},
		getSchemaCache: function() {
			return cache.schema || {};
		},
		setSchemaCache: function(schema) {
			cache.schema = schema || {};
		},
		clearSchemaCache: function() {
			logger.info(`Clearing Tables schema cache`);
			cache.schema = {};
		},
		streamToTableFromS3: function(table, opts) {

		},
		streamToTableBatch: function(table, opts) {
			opts = Object.assign({
				records: 10000,
				useReplaceInto: false
			}, opts || {});
			let pending = null;
			let columns = [];
			let ready = false;
			let total = 0;
			client.query(`SELECT column_name 
					FROM information_schema.columns 
					WHERE table_schema = '${config.database}' and table_name = ${escapeValue(table)} order by ordinal_position asc`, (err, results) => {
				columns = results.map(r => r.column_name);
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
					console.log("Replace Inserting " + records.length + " records of ", total);

				} else {
					console.log("Inserting " + records.length + " records of ", total);
				}
				total += records.length;

				var values = records.map((r) => {
					return columns.map(f => r[f]);
				});

				let cmd = "INSERT INTO ";
				if (opts.useReplaceInto) {
					cmd = "REPLACE INTO ";
				}
				client.query(`${cmd} ${config.database}.${escapeId(table)} (??) VALUES ?`, [ columns, values ], function(err) {
					if (err) {
						callback(err);
					} else {
						callback(null, []);
					}
				});
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(table, opts) {
			opts = Object.assign({
				records: 10000
			});
			return this.streamToTableBatch(table, opts);
		},
		range: function(table, id, opts, callback) {
			client.query(`select min(??) as min, max(??) as max, count(??) as total from ??`, [id, id, id, table], (err, result) => {
				if (err) return callback(err);
				callback(null, {
					min: result[0].min,
					max: result[0].max,
					total: result[0].total
				});
			});
		},
		nibble: function(table, id, start, min, max, limit, reverse, callback) {
			let sql;
			let params;
			if (reverse) {
				sql = `select ?? as id from ??
							where ?? <= ? and ?? >= ?
							ORDER BY ?? desc
							LIMIT ${limit-1},2`;
				params = [id, table, id, start, id, min, id];
			} else {
				sql = `select ?? as id from ??
							where ?? >= ? and ?? <= ?
							ORDER BY ?? asc
							LIMIT ${limit-1},2`;
				params = [id, table, id, start, id, max, id];
			}

			client.query(sql, params, callback);
		},
		getIds: function(table, id, start, end, reverse, callback) {
			let sql;
			if (reverse) {
				sql = `select ?? as id from ??  
                            where ?? <= ? and ?? >= ?
                            ORDER BY ?? desc
                        `;
			} else {
				sql = `select ?? as id from ??  
                            where ?? >= ? and ?? <= ?
                            ORDER BY ?? asc
                        `;
			}
			client.query(sql, [id, table, id, start, id, end, id], callback);
		},
		escapeId,
		escape: function(value) {
			if (value.replace) {
				return '`' + value.replace('`', '') + '`';
			} else {
				return value;
			}
		},
		escapeValue,
		escapeValueNoToLower: function(value) {
			if (value.replace) {
				return "'" + value.replace("'", "\\'") + "'";
			} else {
				return value;
			}
		}
	};

	function escapeId (field) {
		return '`' + field.replace('`', '').replace(/\.([^.]+)$/, '`.`$1') + '`';
	}

	function escapeValue (value) {
		if (value.replace) {
			return "'" + value.replace("'", "\\'").toLowerCase() + "'";
		} else {
			return value;
		}
	}

	function setAuditdate () {
		client.auditdate = "'" + new Date().toISOString().replace(/\.\d*Z/, '').replace(/[A-Z]/, ' ') + "'";
	}

	return client;
};

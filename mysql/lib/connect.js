"use strict";

const mysql = require("mysql2");
const logger = require("leo-logger")("leo.connector.sql.mysql");
let ls = require("leo-sdk").streams;
let connections = {};

module.exports = function(c) {
	let config = Object.assign({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "datawarehouse",
		password: "a",
		connectionLimit: 10,
		timezone: 'utc'
	}, c);

	let connectionHash = JSON.stringify(config);
	let m;

	if (!(connectionHash in connections)) {
		console.log("CREATING NEW MYSQL CONNECTION");
		connections[connectionHash] = mysql.createPool(config);
	} else {
		console.log("REUSING CONNECTION");
	}
	m = connections[connectionHash];

	let queryCount = 0;
	let client = {
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
			log.info(`SQL query #${queryId} is `, query.slice(0, 300));
			log.time(`Ran Query #${queryId}`);

			m.query({
				sql: query,
				rowsAsArray: opts.inRowMode
			}, params, function(err, result, dbfields) {
				log.timeEnd(`Ran Query #${queryId}`);
				let fields;
				if (err) {
					log.error("Had error #${queryId}", query, err);
				} else if (dbfields) {
					// make fields interchangeable between mysql and mysql2 node modules
					fields = dbfields.map(data => {
						let startingObj = {
							type: data.columnType,
							db: data.schema,
							length: data.columnLength,
							schema: ''
						};

						Object.keys(data).filter(f => !f.match(/^\_/)).filter(f => data[f]).map(k => {
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
			return m.end(callback);
		},
		disconnect: function(callback) {
			return this.end(callback);
		},
		describeTable: function(table, callback) {
			client.query(`SELECT column_name, data_type, is_nullable, character_maximum_length 
				FROM information_schema.columns
				WHERE table_name = ? order by ordinal_position asc`, [table], (err, result) => {
				callback(err, result);
			});
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
					WHERE table_name = ? order by ordinal_position asc`, [table], (err, results) => {
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
				client.query(`${cmd} ?? (??) VALUES ?`, [table, columns, values], function(err) {
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
		}
	};

	return client;
};

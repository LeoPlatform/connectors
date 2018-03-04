"use strict";

const mysql = require("mysql");
const logger = require("leo-sdk/lib/logger")("leo.connector.sql.mysql");
let ls = require("leo-sdk").streams;

module.exports = function(config) {
	let m = mysql.createPool(Object.assign({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "datawarehouse",
		password: "a",
		connectionLimit: 10,
		timezone: 'utc'
	}, config));
	let queryCount = 0;
	let client = {
		query: function(query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			m.query(query, params, function(err, result, fields) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error #${queryId}", err);
				}
				callback(err, result, fields);
			});
		},
		disconnect: m.end.bind(m),
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
				records: 10000
			});
			let pending = null;
			let columns = [];
			let ready = false;
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
				console.log("Inserting " + records.length + " records");
				var values = records.map((r) => {
					return columns.map(f => r[f]);
				});
				client.query("INSERT INTO ?? (??) VALUES ?", [table, columns, values], function(err) {
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
		}
	};
	return client;
};

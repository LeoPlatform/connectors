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
					log.info("Had error", err);
				}
				callback(err, result, fields);
			})
		},
		disconnect: m.end.bind(m),
		streamToTableFromS3: function(table, fields, opts) {

		},
		streamToTable: function(table, fields, opts) {
			opts = Object.assign({
				records: 10000
			});
			let fieldColumnLookup = fields.reduce((lookups, f, index) => {
				lookups[f.toLowerCase()] = index;
				return lookups;
			}, {});
			let columns = Object.keys(fieldColumnLookup);
			return ls.bufferBackoff((obj, done) => {
				done(null, obj, 1, 1);
			}, (records, callback) => {
				console.log("Inserting " + records.length + " records");
				var values = records.map((r) => {
					return columns.map(f => r[f]);
				});
				client.query("INSERT INTO ?? (??) VALUES ?", [table, fields, values], function(err) {
					if (err) {
						callback(err);
					} else {
						callback(null, []);
					}
				})
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		}
	};
	return client;
};
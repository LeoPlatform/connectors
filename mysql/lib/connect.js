"use strict";

const mysql = require("mysql");
const logger = require("leo-sdk/lib/logger")("connector.sql.mysql");
let ls = require("leo-sdk").streams;

module.exports = function(config) {
	let m = mysql.createPool(Object.assign({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "datawarehouse",
		password: "a",
		connectionLimit: 10
	}, config));
	let queryCount = 0;
	return {
		query: function(query, callback) {
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			m.query(query, function(err, result, fields) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error", err);
				}
				callback(err, result, fields);
			})
		},
		disconnect: m.end,
		streamToTable: function(table, fields, opts) {
			return ls.bufferBackoff((obj, done) => {
				done(null, obj, 1, 1);
			}, (records, callback) => {
				console.log(records.length);
				return callback(null, []);
				var values = records.map((r) => {
					return [r.event, r.eid, r.gzip.toString('base64'), r.gzipSize, r.size, r.records];
				});
				m.query("INSERT delayed ignore INTO Leo_Stream (event, eid, payload, gzipsize, size, records) VALUES ?", [values], function(err) {
					if (err) {
						console.log(err);
					}
					callback(null, []);
				})

			});
		}
	};
};
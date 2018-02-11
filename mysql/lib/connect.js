"use strict";

const mysql = require("mysql");
const logger = require("leo-sdk/lib/logger")("connector.sql.mysql");

module.exports = function(config) {
	let m = mysql.createPool(Object.assign({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "datawarehouse",
		password: "a",
		connectionLimit: 10
	}));
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
					log.error("Had error", err);
				}
				callback(err, result, fields);
			})
		},
		disconnect: m.end
	};
};
"use strict";

const {
	Pool,
	Client
} = require('pg')
const logger = require("leo-sdk/lib/logger")("connector.sql.postgres");
const sqlLoader = require("../../lib/sql/loader");

const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = function(config, sql, domain) {
	return sqlLoader(() => {
			const pool = new Pool(Object.assign({
				user: 'root',
				host: 'localhost',
				database: 'test',
				password: 'a',
				port: 5432,
			}, config));

			let queryCount = 0;
			let client = {
				query: function(query, callback) {
					let queryId = ++queryCount;
					let log = logger.sub("query");
					log.info(`SQL query #${queryId} is `, query);
					log.time(`Ran Query #${queryId}`);
					pool.query(query, function(err, result) {
						log.timeEnd(`Ran Query #${queryId}`);
						if (err) {
							log.error("Had error", err);
							callback(err);
						} else {
							callback(null, result.rows, result.fields);
						}
					})
				},
				end: pool.close
			};

			return client;
		},
		sql, domain);
};
"use strict";

const elasticsearch = require("elasticsearch");
const logger = require("leo-sdk/lib/logger")("leo.connector.elasticsearch");
let leo = require("leo-sdk")
let ls = leo.streams;
var aws = require("leo-sdk/lib/leo-aws");


module.exports = function (config) {
	let m;
	if (config && config.search) {
		m = config;
	} else {
		m = elasticsearch.Client(Object.assign({
			hosts: host,
			connectionClass: require('http-aws-es'),
			awsConfig: new aws.Config({
				region: leo.configuration.region,
				credentials: config.credentials
			})
		}, config));
	}

	let queryCount = 0;
	let client = Object.assign(m, {
		query: function (query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`Elasticsearch query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			m.search(query, function (err, result) {
				let fields = {};
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error #${queryId}", err);
				}
				callback(err, result, fields);
			});
		},
		disconnect: () => {},
		describeTable: function (table, callback) {
			throw new Error("Not Implemented");
		},
		streamToTableFromS3: function (table, opts) {
			throw new Error("Not Implemented");
		},
		streamToTableBatch: function (table, opts) {
			throw new Error("Not Implemented");
		},
		streamToTable: function (table, opts) {
			opts = Object.assign({
				records: 10000
			});
			return this.streamToTableBatch(table, opts);
		}
	});
	return client;
};
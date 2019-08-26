"use strict";

const elasticsearch = require("elasticsearch");
const logger = require("leo-logger")("leo.connector.elasticsearch");
let leo = require("leo-sdk")
let ls = leo.streams;
var aws = require("leo-sdk/lib/leo-aws");

module.exports = function(config) {
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
		query: function(query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`Elasticsearch query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			m.search(query, function(err, result) {
				let fields = {};
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error #${queryId}", err);
				}
				callback(err, result, fields);
			});
		},
		disconnect: () => {},
		describeTable: function(table, callback) {
			throw new Error("Not Implemented");
		},
		describeTables: function(callback) {
			throw new Error("Not Implemented");
		},
		streamToTableFromS3: function(table, opts) {
			throw new Error("Not Implemented");
		},
		streamToTableBatch: function(opts) {
			opts = Object.assign({
				records: 1000
			}, opts || {});

			return ls.bufferBackoff((obj, done) => {
				done(null, obj, 1, 1);
			}, (records, callback) => {
				logger.log("Inserting " + records.length + " records");
				let body = records.map(data => {
					let isDelete = (data.delete == true);
					let cmd = JSON.stringify({
						[isDelete ? "delete" : "update"]: {
							_index: data.index || data._index,
							_type: data.type || data._type,
							_id: data.id || data._id
						}
					}) + "\n";
					let doc = !isDelete ? (JSON.stringify({
						doc: data.doc,
						doc_as_upsert: true
					}) + "\n") : "";
					return cmd + doc;
				}).join("");

				client.bulk({
					body: body,
					fields: false,
					_source: false
				}, function(err, data) {
					if (err || data.errors) {
						if (data && data.Message) {
							err = data.Message;
						} else if (data && data.items) {
							logger.error(data.items.filter((r) => {
								return 'error' in r.update;
							}).map(e => JSON.stringify(e, null, 2)));
							err = "Cannot load";
						} else {
							logger.error(err);
							err = "Cannot load";
						}
					}
					callback(err, []);
				});
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(opts) {
			opts = Object.assign({
				records: 1000
			}, opts || {});
			return this.streamToTableBatch(opts);
		}
	});
	return client;
};

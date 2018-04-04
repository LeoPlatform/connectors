"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlLoaderJoin = require("leo-connector-common/sql/loaderJoinTable.js");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const sqlSnapshotter = require("leo-connector-common/sql/snapshotter");
const snapShotter = require("leo-connector-common/sql/snapshotter");

const leo = require("leo-sdk");
const ls = leo.streams;

const binlogReader = require("./lib/binlogreader");
module.exports = {
	query: function(config, sql, params) {
		const client = connect(config);
		const results = new Promise((resolve, reject)=> {
			client.query(sql, params, (err, queryResults) => err ? reject(err) : resolve(queryResults));
		})
		return { results, client }
	load: function(config, sql, domain, idColumns) {
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(connect(config), idColumns, sql, domain);
		} else {
			return sqlLoader(connect(config), sql, domain);
		}
	},
	load: function(config, sql, domain, idColumns) {
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(connect(config), idColumns, sql, domain);
		} else {
			return sqlLoader(connect(config), sql, domain);
		}
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
	},
	snapshot: function(config, table, id, domain) {
		return sqlSnapshotter(connect(config), table, id, domain);
	},
	domainObjectLoader: function(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			snapShotter(bot_id, connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
				event: opts.outQueue
			}, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue);
			let stats = ls.stats(bot_id, opts.inQueue);
			ls.pipe(stream, this.load(dbConfig, sql, domain, dbConfig.id), leo.load(bot_id, opts.outQueue || dbConfig.table), err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
		}
	},
	streamChanges: binlogReader.stream
};

"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const snapShotter = require("leo-connector-common/sql/snapshotter");
const checksum = require("leo-connector-common/checksum");
const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(connect(config), sql, domain);
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
	},
	checksum: function(config) {
		return checksum(connect(config));
	},
	domainObjectLoader: function(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			snapShotter(bot_id, connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
				event: opts.outQueue
			}, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue);
			let stats = ls.stats(bot_id, opts.inQueue);
			ls.pipe(stream, this.load(dbConfig, sql, domain), leo.load(bot_id, opts.outQueue || dbConfig.table), err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
		}
	},
	connect: connect
};

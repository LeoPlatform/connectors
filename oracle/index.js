"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlLoaderJoin = require('leo-connector-common/sql/loaderJoinTable');
const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = {
	load: function(config, sql, domain, opts, idColumns) {
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(connect(config), idColumns, sql, domain, opts);
		} else {
			return sqlLoader(connect(config), sql, domain, opts);
		}
	},
	domainObjectLoader: function(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			throw new Error('Snapshotting not implemented yet in Oracle');
			// snapShotter(bot_id, connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
			// 	event: opts.outQueue
			// }, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue, {
				start: opts.start
			});
			let stats = ls.stats(bot_id, opts.inQueue);

			let params = [stream, stats];
			if (opts.transform) {
				params.push(opts.transform);
			}
			params.push(this.load(dbConfig, sql, domain, opts, dbConfig.id));
			params.push(leo.load(bot_id, opts.outQueue || dbConfig.table));
			params.push(err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
			ls.pipe.apply(ls, params);
		}
	},
	connect: connect
};

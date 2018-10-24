"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlLoaderJoin = require("leo-connector-common/sql/loaderJoinTable");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const sqlSnapshotter = require("leo-connector-common/sql/snapshotter");
const snapShotter = require("leo-connector-common/sql/snapshotter");
const checksum = require("./lib/checksum.js");
const dol = require("leo-connector-common/dol");
const leo = require("leo-sdk");
const ls = leo.streams;

function getConnection(config) {
	if (!config) {
		throw new Error('Missing database connection credentials');
	} else if (typeof config.query !== "function") {
		config = connect(config);
	}

	return config;
}

module.exports = {
	load: function(config, sql, domain, idColumns, opts) {
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(connect(config), idColumns, sql, domain, opts);
		} else {
			return sqlLoader(connect(config), sql, domain, opts);
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
			let stream = leo.read(bot_id, opts.inQueue, {
				start: opts.start,
				maxOverride: opts.terminateAt
			});
			let stats = ls.stats(bot_id, opts.inQueue);
			let destination = (opts.devnull) ? ls.devnull('here') : leo.load(bot_id, opts.outQueue || dbConfig.table);

			ls.pipe(stream, stats, this.load(dbConfig, sql, domain, dbConfig.id, {
				source: opts.inQueue,
				queue: opts.outQueue,
				id: bot_id,
				limit: opts.limit
			}), destination, err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
		}
	},
	connect: connect,
	checksum: function(config, fieldsTable) {
		return checksum(connect(config), fieldsTable);
	},
	domainObjectBuilder: (config) => {
		return new dol(getConnection(config));
	},
};

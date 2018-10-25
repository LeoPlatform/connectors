'use strict';

const dol = require('../dol');
const sqlLoader = require('../sql/loader');
const sqlLoaderJoin = require('../sql/loaderJoinTable');
const sqlNibbler = require('../sql/nibbler');
const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = class Connector {
	constructor() {
	}

	connect(config) {
		return this.lib_connect(config);
	}

	getConnection(config) {
		if (!config) {
			throw new Error('Missing database connection credentials');
		} else if (typeof config.query !== "function") {
			config = this.connect(config);
		}

		return config;
	}

	load(config, sql, domain, opts, idColumns) {
		let client = this.connect(config);
		if (Array.isArray(idColumns)) {
			// make sure we don't include a values keyword in the return array of ids
			opts.values = false;
			return sqlLoaderJoin(client, idColumns, sql, domain, opts);
		} else {
			return sqlLoader(client, sql, domain, opts);
		}
	}

	nibble(config, table, id, opts) {
		return sqlNibbler(this.connect(config), table, id, opts);
	}

	checksum(config) {
		return this.lib_checksum(this.connect(config));
	}

	streamChanges(config, opts = {}) {
		return this.lib_listener(config, opts);
	}

	domainObjectBuilder(config) {
		return new dol(this.getConnection(config));
	}

	// @deprecated
	dol(config) {
		return this.domainObjectBuilder(config);
	}

	// @deprecated
	DomainObjectBuilder(config) {
		 return this.domainObjectBuilder;
	}

	// @deprecated
	domainObjectLoader(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			snapShotter(bot_id, this.connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
				event: opts.outQueue
			}, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue, {start: opts.start});
			let stats = ls.stats(bot_id, opts.inQueue);

			ls.pipe(stream, stats, this.load(dbConfig, sql, domain, opts, dbConfig.id), leo.load(bot_id, opts.outQueue || dbConfig.table), err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
		}
	}
};

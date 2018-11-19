'use strict';

const dol = require('./dol');
const sqlLoader = require('./sql/loader');
const sqlLoaderJoin = require('./sql/loaderJoinTable');
const sqlNibbler = require('./sql/nibbler');
const snapshotter = require('./sql/snapshotter');

module.exports = class Connector {
	constructor(params) {
		this.params = Object.assign({
			connect: undefined,
			checksum: undefined,
			listener: undefined,
		}, params);
	}

	connect(config) {
		if (!config) {
			throw new Error('Missing database connection credentials');
		} else if (typeof config.query !== "function") {
			config = this.params.connect(config);
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
		return this.params.checksum(this.connect(config));
	}

	streamChanges(config, opts = {}) {
		return this.params.listener(config, opts);
	}

	domainObjectBuilder(config) {
		return new dol(this.connect(config));
	}

	snapshotter(config) {
		return new snapshotter(this.connect(config));
	}

	// @deprecated
	dol(config) {
		console.log('`dol` is deprecated and will be removed. Please use `domainObjectBuilder` instead.');

		return this.domainObjectBuilder(config);
	}

	// @deprecated
	DomainObjectLoader(config) {
		console.log('`DomainObjectLoader` is deprecated and will be removed. Please use `domainObjectBuilder` instead.');

		return this.domainObjectBuilder(config);
	}
};

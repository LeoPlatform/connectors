"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum.js");
const binlogReader = require("./lib/binlogreader");
const parent = require('leo-connector-common/base');
const dol = require('./lib/dol');

class connector extends parent {

	constructor() {
		super({
			connect: connect,
			checksum: checksum,
		});

		this.streamChanges = binlogReader.stream;
	}

	domainObjectBuilder(config) {
		return new dol(this.connect(config));
	}
}

module.exports = new connector;

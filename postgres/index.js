"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum.js");
const binlogReader = require("./lib/binlogreader");

const parent = require('leo-connector-common/base');

class connector extends parent {

	constructor() {
		super({
			connect: connect,
			checksum: checksum,
		});
	}

	static streamChanges() {
		return binlogReader.stream;
	}
}

module.exports = new connector;

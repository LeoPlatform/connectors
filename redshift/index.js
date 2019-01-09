"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum.js");
const parent = require('leo-connector-common/base');

class connector extends parent {

	constructor() {
		super({
			connect: connect,
			checksum: checksum,
		});
	}
}

module.exports = new connector;

"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum");
const listener = require('./lib/listener');
const parent = require('leo-connector-common/base');

class connector extends parent {

	constructor() {
		super({
			connect: connect,
			checksum: checksum,
			listener: listener
		});
	}
}

module.exports = new connector;

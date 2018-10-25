"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum");
const listener = require('./lib/listener');
const parent = require('leo-connector-common/base');

class connector extends parent {

	constructor() {
		super();
		super.lib_connect = connect;
		super.lib_checksum = checksum;
		super.lib_listener = listener;
	}
}

module.exports = new connector;

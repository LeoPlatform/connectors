"use strict";
const connect = require("./lib/connect.js");
const parent = require('leo-connector-common/base');

class connector extends parent {

	constructor() {
		super();
		super.lib_connect = connect;
	}
}

module.exports = new connector;

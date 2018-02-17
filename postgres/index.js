"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");

const leo = require("leo-sdk");
const ls = leo.streams;
module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	}
};
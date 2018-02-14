"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	}
};
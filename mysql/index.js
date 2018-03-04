"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");
const sqlNibbler = require("../lib/sql/nibbler");

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	},
	nibble: function(config, table, id) {
		return sqlNibbler(connect(config), table, id);
	}
};

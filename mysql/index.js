"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
	}
};

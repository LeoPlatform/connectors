"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const sqlSnapshotter = require("leo-connector-common/sql/snapshotter");

const binlogReader = require("./lib/binlogreader");

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(connect(config), sql, domain);
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
	},
	snapshot: function(config, table, id, domain) {
		return sqlSnapshotter(connect(config), table, id, domain);
	},
	streamChanges: binlogReader.stream
};

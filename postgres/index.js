"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const sqlSnapshotter = require("leo-connector-common/sql/snapshotter");

const binlogReader = require("./lib/binlogReader.js");



const leo = require("leo-sdk");
const ls = leo.streams;



let lastLsn = '0/00000000';


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
	streamChanges: function(config, table, id, domain, opts) {
		opts = Object.assign({
			slot_name: 'leo_replication',
			keepalive: 1000 * 10
		}, opts || {});
		let stream = binlogReader.stream(config, opts.slot_name);

		let count = 0;
		ls.pipe(stream, ls.through((obj, done) => {
			count++;
			if (count % 10000 === 0) {
				console.log(count);
			}
			done();
		}), ls.devnull(), (err) => {
			console.log(err);
		});
	}
};

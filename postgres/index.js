"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");

const LogicalReplication = require('pg-logical-replication')

const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	},
	binlogReader: function(connection, slot_name = 'bus_replication') {
		var stream = new LogicalReplication(connection);
		var PluginTestDecoding = LogicalReplication.LoadPlugin('output/test_decoding');

		let lastLsn = null;



		let thr = ls.through((obj, done) => {
			if (!('data' in obj)) {
				return done();
			} else if (obj.action == "DELETE") {
				return done();
			}
			try {
				done(null, {
					s: obj.schema,
					t: obj.table,
					a: obj.action,
					d: obj.data.reduce((acc, e) => {
						acc[e.name] = e.value;
						return acc;
					}, {})
				})
			} catch (e) {
				console.log(e);
			}
		});


		stream.on("data", (msg) => {
			lastLsn = msg.lsn || lastLsn;
			var log = (msg.log || '').toString('utf8');
			try {
				thr.write(PluginTestDecoding.parse(log));
			} catch (e) {
				console.trace(log, e);
			}
		}).on("error", (err) => {
			console.log(err);
		});

		ls.pipe(thr, ls.stringify(), require("fs").createWriteStream("/tmp/changes"), (err) => {
			console.log(err);
		});

		stream.getChanges(slot_name, null, function(err) {
			console.log('Logical replication initialize error', err);
		});
	}
};
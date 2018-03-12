"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const sqlSnapshotter = require("leo-connector-common/sql/snapshotter");
const LogicalReplication = require('pg-logical-replication');

const leo = require("leo-sdk");
const ls = leo.streams;

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
	binlogReader: function(connection, slot_name = 'bus_replication', lastLsn = null) {
		var stream = new LogicalReplication(connection);
		var PluginTestDecoding = LogicalReplication.LoadPlugin('output/test_decoding');

		let thr = ls.through((obj, done) => {
			const {log} = obj;
			if (!('data' in log)) {
				//rejecting BEGIN and COMMIT
				//TODO: Can the messages be buffered in order to get the COMMIT timestamp and apply it to all rows in the commit? 
				//      Accepting the data after a commit makes more sense anyway. There may be buffer size issues for large transactions however
				return done();
			} else if (log.action === "DELETE") {
				return done();
			}
			try {
				done(null, {
					lsn: obj.lsn,
					log: {
						s: log.schema,
						t: log.table,
						a: log.action,
						d: log.data.reduce((acc, e) => {
							acc[e.name] = e.value;
							return acc;
						}, {})
					}
				});
			} catch (e) {
				console.log("simplify transformation error", e);
				done(e);
			}
		});

		stream.on("data", (msg) => {
			lastLsn = msg.lsn || lastLsn;
			var logStr = (msg.log || '').toString('utf8');
			var log = PluginTestDecoding.parse(logStr.replace(/integer\[\]/g, "integer"));
			try {
				thr.write({
					lsn: msg.lsn,
					log
				});
			} catch (e) {
				console.trace("Error writing data through", e);
				stream.stop();
				thr.end();
				thr.emit("error", e);
				throw e;
			}
		}).on("error", (err) => {
			console.log("EventEmmitter Error", err);
			stream.stop();
		});

		stream.getChanges(slot_name, lastLsn, {
			includeXids: true,
			includeTimestamp: true
		}, function(err) {
			thr.end();
			thr.emit("error", err);
		});

		return thr;
	}
};

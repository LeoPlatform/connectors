"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const LogicalReplication = require('pg-logical-replication');


const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
	},
	binlogReader: function(connection, slot_name = 'bus_replication', lastLsn = null) {
		var stream = new LogicalReplication(connection);
		var PluginTestDecoding = LogicalReplication.LoadPlugin('output/test_decoding');

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
				});
			} catch (e) {
				console.log("simplify transformation error", e);
				done(e);
			}
		});
		stream.on("data", (msg) => {
			lastLsn = msg.lsn || lastLsn;
			var log = (msg.log || '').toString('utf8');
			try {
				thr.write(PluginTestDecoding.parse(log.replace(/integer\[\]/g, "integer")));
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

		stream.getChanges(slot_name, lastLsn, null, function(err) {
			console.log('getChanges done');
			thr.end();
			thr.emit("error", err);
			if (err)
				console.log('Logical replication initialize error', err);
		});

		return thr;
	}
};

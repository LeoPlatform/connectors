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
	},
	changeTableStructure: function(config, structures, callback) {
		let client = connect(config);

		console.log(client);
		Object.keys(structures).forEach(table => {
			client.describeTable(table, (err, fields) => {
				if (!fields.length) {
					client.createTable(table, structures[table]);
				} else {
					let fieldLookup = fields.reduce((acc, field) => {
						acc[field.column_name] = field;
						return acc;
					}, {});
					let missingFields = {};
					Object.keys(structures[table].structure).forEach(f => {
						if (!(f in fieldLookup)) {
							missingFields[f] = structures[table][f];
						}
					})
					console.log(missingFields);

					if (Object.keys(missingFields).length) {
						client.updateTable(table, missingFields);
					}
				}
			})
		});
	}
};
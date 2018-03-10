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

		return;


		let rawClient = new pg.Client(Object.assign({}, config, {
			replication: 'database'
		}));
		rawClient.on('error', function(err) {
			console.log(err);
		});
		let client = connect(config);


		client.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
			console.log(err, result);
			if (!result.length) {
				client.query(`SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')`, [opts.slot_name], (err, result) => {

				});
			} else {
				rawClient.connect(function(err) {
					rawClient.query(`START_REPLICATION SLOT leo_replication LOGICAL ${lastLsn}`, (err, result) => {
						console.log("HERE");
						console.log(err, result);
					});
					rawClient.connection.once('replicationStart', function() {
						let e = function() {
							standbyStatusUpdate(rawClient);
							setTimeout(e, opts.keepalive);
						};
						e();
						rawClient.connection.on('copyData', function(msg) {
							if (msg.chunk[0] == 0x77) { // XLogData
								let lsn = (msg.chunk.readUInt32BE(1).toString(16).toUpperCase()) + '/' + (msg.chunk.readUInt32BE(5).toString(16).toUpperCase());
								lastLsn = lsn;
								console.log({
									lsn,
									log: JSON.stringify(test_decoding.parse(msg.chunk.slice(25).toString('utf8')), null, 2)
								});
							} else if (msg.chunk[0] == 0x6b) { // Primary keepalive message
								let lsn = (msg.chunk.readUInt32BE(1).toString(16).toUpperCase()) + '/' + (msg.chunk.readUInt32BE(5).toString(16).toUpperCase());
								var timestamp = Math.floor(msg.chunk.readUInt32BE(9) * 4294967.296 + msg.chunk.readUInt32BE(13) / 1000 + 946080000000);
								var shouldRespond = msg.chunk.readInt8(17);
								console.log({
									lsn,
									timestamp,
									shouldRespond
								});
							} else {
								console.log('Unknown message', msg.chunk[0]);
							}

						});
					});
				});

			}
		});
	},
	binlogReader: function(config, slot_name = 'leo_replication', lastLsn = null) {


		return;


		var stream = new LogicalReplication(config);
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

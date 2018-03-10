const pg = require("pg");
const PassThrough = require("stream").PassThrough;
const async = require("async");

const connect = require("./connect.js");
const test_decoding = require("./test_decoding.js");

let count = 1;

module.exports = {
	stream: function(config, opts) {
		opts = Object.assign({
			slot_name: 'leo_replication',
			keepalive: 1000 * 3
		}, opts || {});
		let lastLsn;


		let pass = new PassThrough({
			objectMode: true
		});
		let query = connect(config).query;
		let client = new pg.Client(Object.assign({}, config, {
			replication: 'database'
		}));
		let dieError = function(err) {
			console.log(err);
			pass.emit(err);
			pass.destroy();
			client.removeAllListeners();
			client.end();
		};
		client.on('error', dieError);
		client.connect(function(err) {
			if (err) return dieError(err);
			query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
				if (err) return dieError(err);
				let tasks = [];
				if (!result.length) {
					lastLsn = '0/00000000';
					tasks.push(done => query(`SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')`, [opts.slot_name], err => {
						if (err) return dieError(err);
						query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
							if (err) return dieError(err);
							if (result.length != 1) return dieError(err);

							lastLsn = result[0].confirmed_flush_lsn;
							done();
						});
					}));
				} else {
					lastLsn = result[0].confirmed_flush_lsn;
				}
				async.series(tasks, (err) => {
					console.log("HOWDY");
					if (err) return dieError(err);

					client.query(`START_REPLICATION SLOT leo_replication LOGICAL ${lastLsn}`, (err, result) => {
						if (err) return dieError(err);
					});
					client.connection.once('replicationStart', function() {
						let e = function() {
							standbyStatusUpdate(client, lastLsn);
							setTimeout(e, opts.keepalive);
						};
						e();
						client.connection.on('copyData', function(msg) {
							if (msg.chunk[0] == 0x77) { // XLogData
								count++;
								if (count == 10000) {
									standbyStatusUpdate(client, lastLsn);
									count = 1;
								}

								let lsn = (msg.chunk.readUInt32BE(1).toString(16).toUpperCase()) + '/' + (msg.chunk.readUInt32BE(5).toString(16).toUpperCase());
								lastLsn = lsn;
								pass.write({
									lsn,
									log: test_decoding.parse(msg.chunk.slice(25).toString('utf8'))
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
								if (shouldRespond) {
									standbyStatusUpdate(client, lastLsn);
								}
							} else {
								console.log('Unknown message', msg.chunk[0]);
							}

						});
					});
				});
			});
		});
		return pass;
	}
};

function standbyStatusUpdate(client, lastLsn) {
	let [upperWAL, lowerWAL] = lastLsn.split('/');
	upperWAL = parseInt(upperWAL, 16);
	lowerWAL = parseInt(lowerWAL, 16);

	// Timestamp as microseconds since midnight 2000-01-01
	var now = (Date.now() - 946080000000);
	var upperTimestamp = Math.floor(now / 4294967.296);
	var lowerTimestamp = Math.floor((now - upperTimestamp * 4294967.296));

	if (lowerWAL === 4294967295) { // [0xff, 0xff, 0xff, 0xff]
		upperWAL = upperWAL + 1;
		lowerWAL = 0;
	} else {
		lowerWAL = lowerWAL + 1;
	}

	var response = Buffer.alloc(34);
	response.fill(0x72); // 'r'

	// Last WAL Byte + 1 received and written to disk locally
	response.writeUInt32BE(upperWAL, 1);
	response.writeUInt32BE(lowerWAL, 5);

	// Last WAL Byte + 1 flushed to disk in the standby
	response.writeUInt32BE(upperWAL, 9);
	response.writeUInt32BE(lowerWAL, 13);

	// Last WAL Byte + 1 applied in the standby
	response.writeUInt32BE(upperWAL, 17);
	response.writeUInt32BE(lowerWAL, 21);

	// Timestamp as microseconds since midnight 2000-01-01
	response.writeUInt32BE(upperTimestamp, 25);
	response.writeUInt32BE(lowerTimestamp, 29);

	// If 1, requests server to respond immediately - can be used to verify connectivity
	response.writeInt8(0, 33);
	console.log("sending response", lastLsn, count);

	client.connection.sendCopyFromChunk(response);
}

const pg = require("pg");
const PassThrough = require("stream").PassThrough;
const async = require("async");

const connect = require("./connect.js");
const test_decoding = require("./test_decoding.js");
var backoff = require("backoff");

const logger = require("leo-sdk/lib/logger")("leo-stream");

module.exports = {
	stream: function(ID, config, opts) {
		opts = Object.assign({
			slot_name: 'leo_replication',
			keepalive: 1000 * 3,
			event: 'logical_replication'
		}, opts || {});
		let lastLsn;


		let pass = new PassThrough({
			objectMode: true
		});

		var retry = backoff.fibonacci({
			randomisationFactor: 0,
			initialDelay: 1000,
			maxDelay: 60000
		});
		retry.failAfter(100);
		retry.on('backoff', function(number, delay) {
			logger.error(`Going to try to connect again in ${delay} ms`);
		});
		retry.once('fail', (err) => {
			logger.error(err);
			pass.emit(err);
			pass.destroy();
		});
		let reportBackTimeout = null;

		retry.on('ready', function(number, delay) {
			let wrapperClient = connect(config);
			let client = new pg.Client(Object.assign({}, config, {
				replication: 'database'
			}));
			let dieError = function(err) {
				logger.error(err);
				clearTimeout(reportBackTimeout);
				if (client) {
					client.removeAllListeners();
					try {
						wrapperClient.end(err => {});
					} catch (e) {
						logger.debug("Cannot end WrapperClient");
					}
					try {
						client.end(err => {});
					} catch (e) {
						logger.debug("Cannot end client");
					}
					client = null;
					wrapperClient = null;
					retry.backoff(err);
				}
			};
			client.on('error', dieError);
			client.connect(function(err) {
				logger.debug("Trying to connect ");
				if (err) return dieError(err);
				wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
					if (err) return dieError(err);
					let tasks = [];
					if (!result.length) {
						lastLsn = '0/00000000';
						tasks.push(done => wrapperClient.query(`SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')`, [opts.slot_name], err => {
							if (err) return dieError(err);
							wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
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
						if (err) return dieError(err);

						client.query(`START_REPLICATION SLOT leo_replication LOGICAL ${lastLsn}`, (err, result) => {
							if (err) return dieError(err);
						});
						let [upper, lower] = lastLsn.split('/');
						lastLsn = {
							upper: parseInt(upper, 16),
							lower: parseInt(lower, 16)
						};
						client.connection.once('replicationStart', function() {
							console.log(`Successfully listening for Changes on ${config.host}`);
							retry.reset();
							let e = function() {
								if (reportBackTimeout) {
									clearTimeout(reportBackTimeout);
								}
								checkpoint(client, lastLsn);
								reportBackTimeout = setTimeout(e, opts.keepalive);
							};
							e();
							pass.acknowledge = function(lsn) {
								if (typeof lsn == "string") {
									let [upper, lower] = lsn.split('/');
									lsn = {
										upper: parseInt(upper, 16),
										lower: parseInt(lower, 16)
									};
								}

								if (lsn.lower === 4294967295) { // [0xff, 0xff, 0xff, 0xff]
									lsn.upper = lsn.upper + 1;
									lsn.lower = 0;
								} else {
									lsn.lower = lsn.lower + 1;
								}
								lastLsn = lsn;
								e();
							};
							let count = 0;
							client.connection.on('error', dieError);
							client.connection.on('copyData', function(msg) {
								if (msg.chunk[0] == 0x77) { // XLogData
									count++;
									if (count === 10000) {
										e();
										console.log(count);
										count = 0;
									}
									let lsn = {
										upper: msg.chunk.readUInt32BE(1),
										lower: msg.chunk.readUInt32BE(5),
									};
									lsn.string = lsn.upper.toString(16).toUpperCase() + "/" + lsn.lower.toString(16).toUpperCase();
									//This seems like it was bogus and not needed...I think it was due to a bug of not doing WAL +1 on acknowledge
									// if (lsn.upper > lastLsn.upper || lsn.lower >= lastLsn.lower) { //Otherwise we have already see this one (we died in the middle of a commit
									let log = test_decoding.parse(msg.chunk.slice(25).toString('utf8'));
									log.lsn = lsn;
									if (log.d && log.d.reduce) {
										log.d = log.d.reduce((acc, field) => {
											acc[field.n] = field.v;
											return acc;
										}, {});
									}
									let c = {
										source: 'postgres',
										start: log.lsn.string
									};
									delete log.lsn;
									pass.write({
										id: ID,
										event: opts.event,
										payload: log,
										correlation_id: c
									});
									// }
								} else if (msg.chunk[0] == 0x6b) { // Primary keepalive message
									let lsn = (msg.chunk.readUInt32BE(1).toString(16).toUpperCase()) + '/' + (msg.chunk.readUInt32BE(5).toString(16).toUpperCase());
									var timestamp = Math.floor(msg.chunk.readUInt32BE(9) * 4294967.296 + msg.chunk.readUInt32BE(13) / 1000 + 946080000000);
									var shouldRespond = msg.chunk.readInt8(17);
									logger.info({
										lsn,
										timestamp,
										shouldRespond
									});
									if (shouldRespond) {
										e();
									}
								} else {
									logger.error('Unknown message', msg.chunk[0]);
								}

							});
						});
					});
				});
			});
		});
		retry.backoff();


		return pass;
	}
};

function checkpoint(client, lsn) {
	// Timestamp as microseconds since midnight 2000-01-01
	var now = (Date.now() - 946080000000);
	var upperTimestamp = Math.floor(now / 4294967.296);
	var lowerTimestamp = Math.floor((now - upperTimestamp * 4294967.296));

	var response = Buffer.alloc(34);
	response.fill(0x72); // 'r'

	// Last WAL Byte + 1 received and written to disk locally
	response.writeUInt32BE(lsn.upper, 1);
	response.writeUInt32BE(lsn.lower, 5);

	// Last WAL Byte + 1 flushed to disk in the standby
	response.writeUInt32BE(lsn.upper, 9);
	response.writeUInt32BE(lsn.lower, 13);

	// Last WAL Byte + 1 applied in the standby
	response.writeUInt32BE(lsn.upper, 17);
	response.writeUInt32BE(lsn.lower, 21);

	// Timestamp as microseconds since midnight 2000-01-01
	response.writeUInt32BE(upperTimestamp, 25);
	response.writeUInt32BE(lowerTimestamp, 29);

	// If 1, requests server to respond immediately - can be used to verify connectivity
	response.writeInt8(0, 33);
	logger.debug("sending response", lsn);

	client.connection.sendCopyFromChunk(response);
}

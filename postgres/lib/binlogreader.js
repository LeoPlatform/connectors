const pg = require("pg");
const async = require("async");

const connect = require("./connect.js");
const test_decoding = require("./test_decoding.js");
const ls = require("leo-streams");
const logger = require("leo-logger")("binlogreader");
var backoff = require("backoff");

//I need to overwrite the pg connection listener to apply backpressure;
let Connection = require("pg/lib/connection.js");

let shutdown = false;
let copyDataThrough;
Connection.prototype.attachListeners = function(stream) {
	var self = this;

	stream.on('data', function(buff) {
		self._reader.addChunk(buff);
		var packet = self._reader.read();
		let lastWriteGood = true;
		while (packet) {
			var msg = self.parseMessage(packet);
			if (self._emitMessage) {
				self.emit('message', msg);
			}
			if (msg.name == "copyData") {
				lastWriteGood = copyDataThrough.write(msg);
			} else {
				self.emit(msg.name, msg);
			}
			packet = self._reader.read();
		}

		if (!lastWriteGood || shutdown) {
			stream.pause();
			if (!shutdown) {
				copyDataThrough.once('drain', () => {
					stream.resume();
				});
			}
		}
	});
	stream.on('end', function() {
		self.emit('end');
	});
};



module.exports = {
	stream: function(ID, config, opts) {
		opts = Object.assign({
			slot_name: 'leo_replication',
			keepalive: 1000 * 50,
			failAfter: 100,
			recoverWal: false,
			event: 'logical_replication'
		}, opts || {});
		let lastLsn;
		let startLsn;
		let requestedWalSegmentAlreadyRemoved = false;
		let walCheckpointHeartBeatTimeoutId = null;
		var retry = backoff.fibonacci({
			randomisationFactor: 0,
			initialDelay: 1000,
			maxDelay: 60000
		});
		retry.failAfter(100);
		retry.on('backoff', function(number, delay) {
			logger.error(`(${config.database}) Going to try to connect again in ${delay} ms`);
		});
		retry.once('fail', (err) => {
			err.database = config.database;
			err.traceType = 'fail';
			logger.error(err);
			copyDataThrough.destroy(err);
		});

		let count = 0;
		let replicationClient;

		let maxDate = null;

		copyDataThrough = ls.through((msg, done) => {
			let lsn = {
				upper: msg.chunk.readUInt32BE(1),
				lower: msg.chunk.readUInt32BE(5),
			};

			if (lsn.upper == 0 && lsn.lower == 0) {
				return done(null);
			}
			lsn.string = lsn.upper.toString(16).toUpperCase() + "/" + lsn.lower.toString(16).toUpperCase();
			if (msg.chunk[0] == 0x77) { // XLogData
				count++;
				if (count === 10000) {
					logger.info("every 10000 processed", count, lsn);
					count = 0;
				}
				let log;
				try {
					log = test_decoding.parse(msg.chunk.slice(25).toString('utf8'));
				} catch (err) {
					logger.error("TEST_DECODING ERR", err);
					logger.error("PROBLEMATIC MESSAGE JSON", JSON.stringify(msg));
					logger.error("PROBLEMATIC MESSAGE", msg.chunk.slice(25).toString('utf8'));
					done(err);
				}


				if (log.time) {
					let d = new Date(log.time);
					maxDate = Math.max(d.valueOf(), maxDate);
				}

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
				done(null, {
					id: ID,
					event: opts.event,
					payload: log,
					correlation_id: c,
					event_source_timestamp: maxDate,
					timestamp: Date.now()
				});
			} else if (msg.chunk[0] == 0x6b) { // Primary keepalive message
				let strLastLsn = (lastLsn.upper.toString(16).toUpperCase()) + '/' + (lastLsn.lower.toString(16).toUpperCase());
				var timestamp = Math.floor(msg.chunk.readUInt32BE(9) * 4294967.296 + msg.chunk.readUInt32BE(13) / 1000 + 946080000000);
				var shouldRespond = msg.chunk.readInt8(17);
				logger.debug("Got a keepalive message", {
					lsn,
					timestamp,
					shouldRespond
				});
				if (shouldRespond) {
					logger.debug('Should Respond. LastLsn: ' + strLastLsn + ' THIS lsn: ' + lsn);
					walCheckpoint(replicationClient, lastLsn);
				}
				done(null);
			} else {
				logger.error(`(${config.database}) Unknown message`, msg.chunk[0]);
				done(null);
			}
		});


		retry.on('ready', function() {
			let wrapperClient = connect(config);
			replicationClient = new pg.Client(Object.assign({}, config, {
				replication: 'database'
			}));

			let dieError = function(err) {
				err.database = config.database;
				err.traceType = 'dieError';
				logger.error(err);
				clearTimeout(walCheckpointHeartBeatTimeoutId);
				if (replicationClient) {
					replicationClient.removeAllListeners();
					try {
						wrapperClient.end(err => {
							logger.debug("wrapperClient.end", err);
						});
					} catch (walCheckpointHeartBeat) {
						logger.error(`(${config.database}) Cannot end WrapperClient`);
					}
					try {
						replicationClient.end(err => {
							logger.debug("replicationClient.end", err);
						});
					} catch (walCheckpointHeartBeat) {
						logger.error(`(${config.database}) Cannot end replicationClient`);
					}
					replicationClient = null;
					wrapperClient = null;
					retry.backoff(err);
				}
			};
			replicationClient.on('error', dieError);
			replicationClient.connect(async function(err) {
				logger.info(`(${config.database}) Trying to connect.`);
				if (err) return dieError(err);
				if (opts.recoverWal && requestedWalSegmentAlreadyRemoved) {
					logger.info(`RECOVER FROM WAL SEGMENT ALREADY REMOVED. (removing slot ${opts.slot_name})`);
					const dropSlotPromise = new Promise((resolve, reject) => {
						wrapperClient.query(`SELECT pg_drop_replication_slot($1);`, [opts.slot_name], (err) => {
							if (err) return reject(err);
							resolve();
						});
					});
					try {
						await dropSlotPromise;
						logger.info(`SLOT ${opts.slot_name} REMOVED`);
					} catch (err) {
						dieError(err);
					}
				}
				wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
					logger.info(`(${config.database}) Trying to get replication slot ${opts.slot_name}.`);
					if (err) return dieError(err);
					let tasks = [];
					lastLsn = '0/00000000';
					logger.debug(result);
					if (!result.length) {
						tasks.push(done => wrapperClient.query(`SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')`, [opts.slot_name], err => {
							logger.info(`(${config.database}) Trying to create logical replication slot ${opts.slot_name}.`);
							if (err) return done(err);
							wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
								logger.info(`(${config.database}) Trying to get newly created replication slot ${opts.slot_name}. Result Len = ${result.length}`);
								if (err) return done(err);
								if (result.length != 1) return done(err);

								lastLsn = result[0].confirmed_flush_lsn || result[0].restart_lsn;
								done();
							});
						}));
					} else {
						lastLsn = result[0].confirmed_flush_lsn || result[0].restart_lsn;
					}
					async.series(tasks, (err) => {
						if (err) return dieError(err);
						logger.info(`START_REPLICATION SLOT ${opts.slot_name} LOGICAL ${lastLsn} ("include-timestamp" '1', include-xids" '0', "skip-empty-xacts" '1')`);
						replicationClient.query(`START_REPLICATION SLOT ${opts.slot_name} LOGICAL ${lastLsn} ("include-timestamp" '1', "include-xids" '0', "skip-empty-xacts" '1')`, (err) => {
							if (err) {
								if (err.code === '58P01') requestedWalSegmentAlreadyRemoved = true;
								if (err.message === "Connection terminated by user") {
									logger.error("Logical replication ended with: ", err.message);
									return;
								}
								return dieError(err);
							}
						});
						logger.debug("WANTING TO RESTART AT ", lastLsn);
						let [upper, lower] = lastLsn.split('/');
						lastLsn = {
							upper: parseInt(upper, 16),
							lower: parseInt(lower, 16)
						};
						startLsn = {
							upper: parseInt(upper, 16),
							lower: parseInt(lower, 16)
						};
						if (startLsn.lower == 0) {
							startLsn.lower = 4294967295;
							startLsn.upper--;
						} else {
							startLsn.lower--;
						}
						replicationClient.connection.once('replicationStart', function() {
							logger.info(`Successfully listening for Changes on ${config.host}:${config.database}`);
							retry.reset();
							let walCheckpointHeartBeat = function() {
								if (walCheckpointHeartBeatTimeoutId) {
									clearTimeout(walCheckpointHeartBeatTimeoutId);
								}
								walCheckpoint(replicationClient, lastLsn);
								walCheckpointHeartBeatTimeoutId = setTimeout(walCheckpointHeartBeat, opts.keepalive);
							};
							walCheckpointHeartBeat();
							copyDataThrough.acknowledge = function(lsn) {
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
								walCheckpointHeartBeat();
							};

							replicationClient.connection.on('error', (err) => {
								if (err.message === "Connection terminated by user") return; //ignore this error
								dieError(err);
							});
						});
					});
				});
			});
		});
		retry.backoff();
		return copyDataThrough;
	}
};


function walCheckpoint(replicationClient, lsn) {
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

	lsn.string = lsn.upper.toString(16).toUpperCase() + "/" + lsn.lower.toString(16).toUpperCase();
	logger.debug("sending response", lsn);

	replicationClient.connection.sendCopyFromChunk(response);
}

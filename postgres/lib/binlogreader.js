const pg = require("pg");
const PassThrough = require("stream").PassThrough;
const async = require("async");

const connect = require("./connect.js");
const test_decoding = require("./test_decoding.js");

const logger = require("leo-sdk/lib/logger")("leo-stream");

module.exports = {
	stream: function(ID, config, opts) {
		opts = Object.assign({
			slot_name: 'leo_replication',
			keepalive: 1000 * 3,
			failAfter: 100,
			recoverWal: false,
			event: 'logical_replication'
		}, opts || {});
		let lastLsn;
		let requestedWalSegmentAlreadyRemoved = false;
		let pass = new PassThrough({
			objectMode: true
		});

		let walCheckpointHeartBeatTimeoutId = null;

		let wrapperClient = connect(config);
		let replicationClient = new pg.Client(Object.assign({}, config, {
			replication: 'database'
		}));

		// let walCheckpointHeartBeat = function() {
		// 	if (walCheckpointHeartBeatTimeoutId) {
		// 		clearTimeout(walCheckpointHeartBeatTimeoutId);
		// 	}
		// 	walCheckpoint(replicationClient, lastLsn);
		// 	walCheckpointHeartBeatTimeoutId = setTimeout(walCheckpointHeartBeat, opts.keepalive);
		// };

		let total = 0;
		let count = 0;
		let isStopped = false;
		global.afterStoppedCount = 0;
		let copyDataFunc = (msg) => {
			if (isStopped) {
				global.afterStoppedCount++;
				return;
			}
			let lsn = {
				upper: msg.chunk.readUInt32BE(1),
				lower: msg.chunk.readUInt32BE(5),
			};
			lsn.string = lsn.upper.toString(16).toUpperCase() + "/" + lsn.lower.toString(16).toUpperCase();
			if (msg.chunk[0] == 0x77) { // XLogData
				count++;
				if (total === 0) console.log('start', lsn.string);
				if (count === 10000) {
					//walCheckpointHeartBeat();
					console.log(count, lsn.string);
					count = 0;
				}
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
					console.log('Should Respond');
					walCheckpoint(replicationClient, lastLsn);
				}
			} else {
				logger.error(`(${config.database}) Unknown message`, msg.chunk[0]);
			}
		};

		pass.stopListening = () => {
			//stop listening for new data. So we can wrap it up.
			console.log('stopListening');
			isStopped = true;
			clearTimeout(walCheckpointHeartBeatTimeoutId);
			replicationClient.removeListener('copyData', copyDataFunc);
			pass.end();
		};

		pass.endLogical = () => {
			console.log('called endLogical', global.afterStoppedCount);
			clearTimeout(walCheckpointHeartBeatTimeoutId);
			if (replicationClient) {
				replicationClient.removeAllListeners();
				try {
					wrapperClient.end(err => {
						logger.debug("wrapperClient.end", err);
					});
				} catch (walCheckpointHeartBeat) {
					logger.debug(`(${config.database}) Cannot end WrapperClient`);
				}
				try {
					replicationClient.end(err => {
						logger.debug("replicationClient.end", err);
					});
				} catch (walCheckpointHeartBeat) {
					logger.debug(`(${config.database}) Cannot end replicationClient`);
				}
				replicationClient = null;
				wrapperClient = null;
			}			
		};

		let dieError = function(err) {
			err.database = config.database;
			err.traceType = 'dieError';
			logger.error(err);
			pass.endLogical();
			// pass.emit('error', err);
			// pass.destroy(err);
		};
		replicationClient.on('error', (err) => { 
			dieError(err);
		});
		replicationClient.connect(async function(err) {
			logger.debug(`(${config.database}) Trying to connect.`);
			if (err) return dieError(err);
			if (opts.recoverWal && requestedWalSegmentAlreadyRemoved) {
				console.log(`RECOVER FROM WAL SEGMENT ALREADY REMOVED. (removing slot ${opts.slot_name})`);
				const dropSlotPromise = new Promise((resolve, reject) => {
					wrapperClient.query(`SELECT pg_drop_replication_slot($1);`, [opts.slot_name], (err) => {
						if (err) return reject(err);
						resolve();
					});
				});
				try {
					await dropSlotPromise;
					console.log(`SLOT ${opts.slot_name} REMOVED`);
				} catch (err) {
					dieError(err);
				}
			}
			wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
				logger.debug(`(${config.database}) Trying to get replication slot ${opts.slot_name}.`);
				if (err) return dieError(err);
				let tasks = [];
				lastLsn = '0/00000000';
				if (!result.length) {
					tasks.push(done => wrapperClient.query(`SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')`, [opts.slot_name], err => {
						logger.debug(`(${config.database}) Trying to create logical replication slot ${opts.slot_name}.`);
						if (err) return done(err);
						wrapperClient.query(`SELECT * FROM pg_replication_slots where slot_name = $1`, [opts.slot_name], (err, result) => {
							logger.debug(`(${config.database}) Trying to get newly created replication slot ${opts.slot_name}. Result Len = ${result.length}`);
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
					replicationClient.query(`START_REPLICATION SLOT ${opts.slot_name} LOGICAL ${lastLsn}  ("include-xids" 'on' , "include-timestamp" 'on')`, (err, result) => {
						if (err) {
							if (err.code === '58P01') requestedWalSegmentAlreadyRemoved = true;
							if (err.message === "Connection terminated by user") {
								console.log("Logical replication ended with: ", err.message);
								return; 
							}
							return dieError(err);
						} 
					});
					let [upper, lower] = lastLsn.split('/');
					lastLsn = {
						upper: parseInt(upper, 16),
						lower: parseInt(lower, 16)
					};
					replicationClient.connection.once('replicationStart', function() {
						console.log(`Successfully listening for Changes on ${config.host}:${config.database}`);
						//walCheckpointHeartBeat();
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
							walCheckpoint(replicationClient, lastLsn);
						};
						replicationClient.connection.on('error', (err)=>{
							if (err.message === "Connection terminated by user") return; //ignore this error
							dieError(err);
						});
						replicationClient.connection.on('copyData', copyDataFunc);
					});
				});
			});
		});

		return pass;
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
	logger.debug("sending response", lsn);

	replicationClient.connection.sendCopyFromChunk(response);
}

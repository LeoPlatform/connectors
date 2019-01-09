'use strict';

const leo = require("leo-sdk");
const leoaws = require('leo-aws');
const ls = require('leo-streams');
const leostream = leo.streams;
const moment = require("moment");
const merge = require('lodash.merge');
const logger = require('leo-logger');

const nibbler = require("./nibbler.js");

const tableName = leo.configuration.resources.LeoCron;

module.exports = class Snapshotter {
	constructor(connector) {
		this.params = {
			connector: connector,
			nibble: null,
			logTimeout: null,
			runUntilComplete: true,
			timestamp: moment(),
			botId: process.__config.registry.context.botId
		};
	}

	/**
	 * Start snapshot reading
	 * @param settings
	 * @returns {*}
	 */
	read(settings) {
		// add the settings parameters
		this.params = merge(this.params, settings);

		if (!this.params.table) {
			throw new Error('No `table` specified.');
		} else if (!this.params.pk) {
			throw new Error('No `pk` specified for the specified table.');
		} else if (!this.params.botId) {
			throw new Error('NO botId specified.');
		}

		let stream = ls.passthrough({
			objectMode: true,
		});

		leoaws.dynamodb.get(tableName, this.params.botId)
		.then(result => {
			ls.pipe(this.nibble(result), this.format(), stream);
		})
		.catch(err => {
			throw new Error(err);
		});

		return stream;
	}

	/*****************************************
	 * Start fancy console log status thingy *
	 *****************************************/
	//@todo: Update all this to use the log-update node module
	clearLog() {
		process.stdout.write("\r\x1b[K");
		if (this.params.logTimeout) clearInterval(this.params.logTimeout);
	}

	log() {
		this.clearLog();
		let percent = (this.params.nibble.progress / this.params.nibble.total) * 100;
		let fixed = percent.toFixed(2);
		if (fixed === "100.00" && percent < 100) {
			fixed = "99.99";
		}
		logger.log(fixed + "% :", Object.keys(arguments).map(k => arguments[k]).join(", "));
	};

	timeLog(message) {
		this.clearLog();
		let time = new Date();

		this.writeMessage(time);
		this.params.logTimeout = setInterval(() => {
			this.writeMessage(time);
		}, 200);
	}

	writeMessage(time) {
		process.stdout.write("\r\x1b[K");
		process.stdout.write(((new Date() - time) / 1000).toFixed(1) + "s : " + message);
	}

	normalLog(message) {
		this.clearLog();
		logger.log(message);
	}
	/***************************************
	 * End fancy console log status thingy *
	 ***************************************/

	/**
	 * Save the current progress data for the snapshot
	 * @param data
	 * @param timestamp
	 */
	saveProgress(data, timestamp) {
		leoaws.dynamodb.merge(tableName, this.params.botId, {
			snapshot: Object.assign({
				last_run: moment.now(),
				bucket_timestamp: timestamp && timestamp.valueOf()
			}, data)
		}).catch(err => {
			throw new Error(err);
		});
	}

	/**
	 * nibble through the database
	 * @param result
	 * @returns {pass}
	 */
	nibble(result) {
		let self = this;
		// reuse an existing bucket key if we’re resuming, otherwise create a new one.
		this.params.timestamp = moment(result && result.snapshot && !result.snapshot.complete && result.snapshot.bucket_timestamp || undefined);

		this.params.resume = result && result.snapshot && !result.snapshot.complete && result.snapshot;
		let stream = nibbler(this.params.connector, this.params.table, this.params.pk, this.params);
		stream.destroy = stream.destroy || stream.close || (() => {});

		stream.on("ranged", function (n) {
			self.params.nibble = n;
			self.saveProgress(self.params.nibble, self.params.timestamp);
		});

		stream.on('end', () => {
			clearTimeout(self.params.streamTimeout);
		});

		this.params.streamTimeout = setTimeout(() => {
			stream.stop();
		}, Math.min(2147483647, moment.duration(self.params.duration || {
			seconds: process.__config.registry.context.getRemainingTimeInMillis() * 0.8
		}).asMilliseconds()));

		return stream;

	}

	/**
	 * Format the payload, include a dummy database.
	 * @todo support multiple databases.
	 * @returns {*}
	 */
	format() {
		return ls.through((obj, done) => {

			let payload = obj.payload[this.params.table];

			// turn this into a format the domainObjectTransform is expecting, complete with the dummy database name
			obj = {
				ids: {
					__database__: payload
				},
				correlation_id: {
					source: 'snapshot',
					units: payload.length,
					start: payload[0],
					end: payload[payload.length - 1]
				}
			};

			done(null, obj);
		});
	}

	// write out the results
	/**
	 *
	 * @param {string} botId
	 * @param {string} destination
	 * @returns {*}
	 */
	write(botId, destination) {
		this.params.destination = destination;
		return ls.pipeline(
			ls.through((event, done)=>{
				event.id = botId;
				event.timestamp = Date.now();
				event.event_source_timestamp = Date.now();
				done(null, event);
			}),
			this.writeToS3(),
			this.writeToLeo(),
			this.writeCheckpoint()
		);
	}

	/**
	 * Write the events to a gzip file and upload to S3
	 * @returns {*}
	 */
	writeToS3() {
		let bucketKey = this.params.timestamp.format('YYYY/MM_DD_') + this.params.timestamp.valueOf();
		return leostream.toS3GzipChunks(this.params.destination, {
			useS3Mode: true,
			time: {
				minutes: 1
			},
			prefix: "_snapshot/" + bucketKey,
			sectionCount: 30
		});
	}

	/**
	 * Write the snapshot event to leo
	 * @returns {*}
	 */
	writeToLeo() {
		return leostream.toLeo('snapshotter', {
			snapshot: this.params.timestamp.valueOf()
		});
	}

	/**
	 * Send a checkpoint command to the kinesis reader to checkpoint at the end.
	 */
	writeCheckpoint() {
		return leostream.cmd("checkpoint", (obj, done) => {
			if (obj.correlations) {
				let records = 0;
				obj.correlations.forEach(c => {
					this.params.nibble.start = c.snapshot.end;
					this.params.nibble.progress += c.snapshot.records;
					records += c.snapshot.records;
				});
				this.log(`Processed  ${records}  ${this.params.nibble.progress}/${this.params.nibble.total}. Remaining ${this.params.nibble.total - this.params.nibble.progress}`);

				let complete = false;
				if (this.params.nibble.end == this.params.nibble.start) {
					complete = true;
				} else if (typeof this.params.nibble.end === 'object') {
					let end = this.params.nibble.end;
					let start = this.params.nibble.start;

					// compare each element of the object
					for (let key in end) {
						if (end[key] == start[key]) {
							complete = true;
						} else {
							// if we hit false at any point, break so we don't keep checking
							complete = false;
							break;
						}
					}
				}

				if (complete) {
					this.params.nibble.complete = true;
					this.saveProgress(this.params.nibble);

					let closeStream = ls.pipeline(leostream.toLeo("snapshotter", {
						snapshot: this.params.timestamp.valueOf()
					}), ls.devnull());

					closeStream.write({
						_cmd: 'registerSnapshot',
						event: this.params.destination,
						start: this.params.timestamp.valueOf(),
						next: this.params.timestamp.clone().startOf('day').valueOf(),
						id: this.params.botId
					});
					closeStream.on("finish", done);
					closeStream.end();
				} else {
					if (this.params.runUntilComplete) {
						leo.bot.runAgain();
					}
					this.saveProgress(this.params.nibble, this.params.timestamp);
					done();
				}
			} else {
				done();
			}
		});
	}
};

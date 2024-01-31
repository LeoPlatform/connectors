const leoaws = require('leo-aws');
let leo = require("leo-sdk");
const logger = require('leo-logger')('leo-connector-entity-table');
let ls = leo.streams;
let refUtil = require("leo-sdk/lib/reference.js");
let merge = require("lodash.merge");
var backoff = require("backoff");
var async = require("async");
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const zlib = require('zlib');

const GZIP_MIN = 5000;

function hashCode(str) {
	if (typeof str === "number") {
		return str;
	} else if (Array.isArray(str)) {
		let h = 0;
		for (let a = 0; a < str.length; a++) {
			h += hashCode(str[a]);
		}
		return h;
	}
	let hash = 0,
		i, chr;
	if (str.length === 0) return hash;
	for (i = 0; i < str.length; i++) {
		chr = str.charCodeAt(i);
		hash = ((hash << 5) - hash) + chr;
		hash |= 0; // Convert to 32bit integer
	}
	return hash;
}

module.exports = {
	/**
	 * Expects a string, deflates it, and converts it to base64
	 * @param string
	 * @returns {string}
	 */
	deflate: string => {
		let buffer = zlib.deflateSync(string);
		return buffer.toString('base64');
	},
	hash: (entity, id, count = 10) => {
		let hash = Math.abs(hashCode(id)) % count;
		return `${entity}-${hash}`;
	},
	/**
	 * Expects a base64 encoded string, decodes it, and inflates it.
	 * @param string
	 * @returns {string}
	 */
	inflate: string => {
		let buffer = Buffer.from(string, 'base64');
		return zlib.inflateSync(buffer).toString();
	},
	get: function(table, entity, id) {
		if (id === undefined) {
			id = entity;
			entity = "";
		} else {
			entity += "-";
		}

		return leoaws.dynamodb.query({
			TableName: table,
			KeyConditionExpression: `#partition = :partition and #id = :id`,
			ExpressionAttributeNames: {
				"#partition": "partition",
				"#id": "id"
			},
			ExpressionAttributeValues: {
				":partition": entity + (hashCode(id) % 10),
				":id": id
			},
			Limit: 1
		}).then(data => (data.Items[0] && data.Items[0].data));
	},
	load: function(table, entity, objFunc, opts = {}) {
		opts = Object.assign({
			records: 25,
			range: 'id',
			hash: 'partition',
			merge: false
		}, opts || {});

		let self = this;
		return ls.pipeline(
			ls.through(async function(obj, done) {
				let e = await objFunc(obj, self.hash);
				if (e === undefined) {
					return done();
				}
				if (!e[opts.range]) {
					throw new Error(`${opts.range} is required`);
				} else if (!e[opts.hash]) {
					throw new Error(`${opts.hash} is required`);
				}
				done(null, e);
			}),
			ls.through((payload, done) => {
				// check size
				let size = Buffer.byteLength(JSON.stringify(payload));

				if (size > GZIP_MIN) {
					let compressedObj = {
						[opts.range]: payload[opts.range],
						[opts.hash]: payload[opts.hash],
						compressedData: self.deflate(JSON.stringify(payload)),
					};
					payload = compressedObj;
				}

				done(null, payload);
			}),
			toDynamoDB(table, opts)
		);
	},
	loadFromQueue: function(table, queue, objFunc, opts) {
		opts = Object.assign({
			records: 25
		}, opts || {});

		return new Promise((resolve, reject) => {
			let entity = refUtil.ref(queue).id;
			let botId = opts.botId || `${entity}_entity_loader`;
			let stats = ls.stats(botId, queue);
			let readOpts = {};
			if (opts.limit) {
				readOpts.limit = opts.limit;
			}
			if (opts.start) {
				readOpts.start = opts.start;
			}
			if (opts.fast_s3_read) {
				readOpts.fast_s3_read = opts.fast_s3_read;
			}
			if (opts.fast_s3_read_parallel_fetch_max_bytes) {
				readOpts.fast_s3_read_parallel_fetch_max_bytes = opts.fast_s3_read_parallel_fetch_max_bytes;
			}
			if (opts.stream_query_limit) {
				readOpts.stream_query_limit = opts.stream_query_limit;
			}
			if (opts.loops) {
				readOpts.loops = opts.loops;
			}
			ls.pipe(
				leo.read(botId, queue, readOpts),
				stats,
				ls.through((event, done) => {
					done(null, event.payload);
				}),
				this.load(table, entity, objFunc, opts),
				(err) => {
					if (err) {
						console.log("Error:", err);
						return reject(err);
					}
					let statsData = stats.get();
					stats.checkpoint((err) => {
						if (err) {
							return reject(err);
						}
						if (statsData.units > 0) {
							let system = opts.system || `system:dynamodb.${table.replace(/-[A-Z0-9]{12,}$/, "")}.${entity}`;
							leo.bot.checkpoint(botId, system, {
								type: "write",
								eid: statsData.eid,
								records: statsData.units,
								started_timestamp: statsData.started_timestamp,
								ended_timestamp: statsData.ended_timestamp,
								source_timestamp: statsData.source_timestamp
							}, () => {
								return resolve();
							});
						} else {
							return resolve();
						}
					});
				}
			);
		});
	},
	tableOldNewProcessor: function(optionsIn) {
		let options = Object.assign({ kinesisBatchLimit: 200 }, optionsIn);
		let self = this;
		return function(event, context, callback) {
			let streams = {};
			let batchSize = event.Records.length;
			let filePrefix = event.Records[0].eventID;
			logger.info(`batchSize=${batchSize}`);
			let localReadOpts = Object.assign({
				prefix: filePrefix,
				force: true
			}, options.read);
			if (batchSize >= options.kinesisBatchLimit) {
				logger.info(`using S3 for batchSize of ${batchSize}`);
				localReadOpts.useS3 = true;
			}
			else {
				logger.info(`using Kinesis for batchSize of ${batchSize}`);
				localReadOpts.useS3 = false;
			}

			let index = 0;
			let defaultQueue = options.defaultQueue || "Unknown";
			let resourcePrefix = sanitizePrefix(options.resourcePrefix);
			let resourceSuffix = options.eventSuffix || "_table_changes";
			let getStream = (id, queue) => {
				let key = `${id}/${queue}`;
				if (!(key in streams)) {
					streams[key] = leo.load(id, queue, localReadOpts);
					// ls.pipeline(leo.write(id), ls.toCheckpoint({
					// 	force: true
					// }));
				}
				return streams[key];
			};
			async.doWhilst(
				function(done) {
					let record = event.Records[index];
					let data = {
						correlation_id: {
							start: record.eventID,
							source: record.eventSourceARN.match(/:table\/(.*?)\/stream/)[1]
						},
						event: defaultQueue,
						event_source_timestamp: record.dynamodb.ApproximateCreationDateTime * 1000,
						id: context.botId,
						payload: {
							new: null,
							old: null
						},
						timestamp: Date.now(),
					};
					let eventPrefix = resourcePrefix;
					if ("OldImage" in record.dynamodb) {
						let image = unmarshall(record.dynamodb.OldImage);

						if (image.compressedData) {
							// compressedData contains everything including hash/range
							let inflated = self.inflate(image.compressedData);
							data.payload.old = JSON.parse(inflated);
						} else {
							data.payload.old = image;
						}

						if (resourcePrefix.length === 0) {
							eventPrefix = image.partition.split(/-/)[0];
						}
					}
					if ("NewImage" in record.dynamodb) {
						let image = unmarshall(record.dynamodb.NewImage);

						if (image.compressedData) {
							// compressedData contains everything including hash/range
							let inflated = self.inflate(image.compressedData);
							data.payload.new = JSON.parse(inflated);
						} else {
							data.payload.new = image;
						}

						if (resourcePrefix.length === 0) {
							eventPrefix = image.partition.split(/-/)[0];
						}
					}
					data.id = `${options.botPrefix || ""}${resourcePrefix}${options.botSuffix || ""}`;
					data.event = `${eventPrefix}${resourceSuffix}`;
					let sanitizedSrc = data.correlation_id.source.replace(/-[A-Z0-9]{12,}$/, "");
					data.correlation_id.source = options.system || `system:dynamodb.${sanitizedSrc}.${eventPrefix}`;

					let stream = getStream(data.id, data.event);
					stream.write(data) ? done() : stream.once("drain", () => done());
				},
				function() {
					index++;
					return index < event.Records.length;
				},
				function(err) {
					if (err) {
						return callback(err);
					}
					async.parallel(Object.keys(streams).map(key => {
						return function(done) {
							streams[key].end(err => {
								done(err);
							});
						};
					}), callback);
				}
			);
		};
		function sanitizePrefix(pfx) {
			return pfx ? pfx.trim().replace(/-$/, "").trim() : "";
		}
	},
	tableProcessor: function(event, context, callback) {	// old function for supporting old way before we had tableOldNewProcessor
		this.tableOldNewProcessor({
			botSuffix: "_entity_changes",
			defaultQueue: "Unknown",
			eventSuffix: "_changes",
		});
	}
};


function toDynamoDB(table, opts) {
	opts = Object.assign({
		records: 25,
		size: 1024 * 1024 * 2,
		time: {
			seconds: 2
		},
		maxAsyncTasks: 10,
		merge: false
	}, opts || {});

	var records, size;

	let keysArray = [opts.hash];
	opts.range && keysArray.push(opts.range);
	let key = opts.range ? (obj) => {
		return `${obj[opts.hash]}-${obj[opts.range]}`;
	} : (obj) => {
		return `${obj[opts.hash]}`;
	};
	let assign = (self, key, obj) => {
		self.data[key] = obj;
		return false;
	};
	if (opts.merge) {
		assign = (self, key, obj) => {
			if (key in self.data) {
				self.data[key] = merge(self.data[key], obj);
				return true;
			} else {
				self.data[key] = obj;
				return false;
			}
		};
	}

	function reset() {
		if (opts.hash || opts.range) {
			records = {
				length: 0,
				data: {},
				push: function(obj) {
					this.length++;
					return assign(this, key(obj), obj);
				},
				map: function(each) {
					return Object.keys(this.data).map(key => each(this.data[key]));
				}
			};
		} else {
			records = [];
		}
	}
	reset();

	var retry = backoff.fibonacci({
		randomisationFactor: 0,
		initialDelay: 100,
		maxDelay: 1000
	});
	retry.failAfter(10);
	retry.success = function() {
		retry.reset();
		retry.emit("success");
	};
	retry.run = function(callback) {
		let fail = (err) => {
			retry.removeListener('success', success);
			callback(err || 'failed');
		};
		let success = () => {
			retry.removeListener('fail', fail);
			reset();
			callback();
		};
		retry.once('fail', fail).once('success', success);

		retry.fail = function(err) {
			retry.reset();
			callback(err);
		};
		retry.backoff();
	};
	retry.on('ready', function(number, delay) {
		if (records.length === 0) {
			retry.success();
		} else {
			logger.info("sending", records.length, number, delay);
			logger.time("dynamodb request");

			let keys = [];
			let lookup = {};
			let all = records.map((r) => {
				let wrapper = {
					PutRequest: {
						Item: r
					}
				};
				if (opts.merge && opts.hash) {
					lookup[key(r)] = wrapper;
					keys.push({
						[opts.hash]: r[opts.hash],
						[opts.range]: opts.range && r[opts.range]
					});
				}
				return wrapper;
			});
			let getExisting = opts.merge ? ((done) => {
				leo.aws.dynamodb.batchGetTable(table, keys, {}, done);
			}) : done => done(null, []);

			let tasks = [];
			for (let ndx = 0; ndx < all.length; ndx += 25) {
				let myRecords = all.slice(ndx, ndx + 25);
				tasks.push(function(done) {
					let retry = {
						backoff: (err) => {
							done(null, {
								backoff: err || "error",
								records: myRecords
							});
						},
						fail: (err) => {
							done(null, {
								fail: err || "error",
								records: myRecords
							});
						},
						success: () => {
							done(null, {
								success: true,
								//records: myRecords
							});
						}
					};
					leo.aws.dynamodb.docClient.batchWrite({
						RequestItems: {
							[table]: myRecords
						},
						"ReturnConsumedCapacity": 'TOTAL'
					}, function(err, data) {
						if (err) {
							logger.info(`All ${myRecords.length} records failed! Retryable: ${err.retryable}`, err);
							logger.error(myRecords);
							if (err.retryable) {
								retry.backoff(err);
							} else {
								retry.fail(err);
							}
						} else if (table in data.UnprocessedItems && Object.keys(data.UnprocessedItems[table]).length !== 0) {
							//reset();
							//data.UnprocessedItems[table].map(m => records.push(m.PutRequest.Item));
							myRecords = data.UnprocessedItems[table];
							retry.backoff();
						} else {
							logger.info(table, "saved");
							retry.success();
						}
					});
				});
			}
			getExisting((err, existing) => {
				if (err) {
					return retry.fail(err);
				}
				existing.map(e => {
					let newObj = lookup[key(e)];
					newObj.PutRequest.Item = merge(e, newObj.PutRequest.Item);
				});
				console.log(`tasks.length = ${tasks && tasks.length || 0}`);
				async.parallelLimit(tasks, opts.maxAsyncTasks || 10, (err, results) => {
					if (err) {
						retry.fail(err);
					} else {
						let fail = false;
						let backoff = false;
						reset();
						results.map(r => {
							fail = fail || r.fail;
							backoff = backoff || r.backoff;
							if (!r.success) {
								r.records.map(m => records.push(m.PutRequest.Item));
							}
						});

						if (fail) {
							retry.fail(fail);
						} else if (backoff) {
							retry.backoff(backoff);
						} else {
							retry.success();
						}
					}
				});
			});
		}
	});
	return ls.buffer({
		writeStream: true,
		label: "toDynamoDB",
		time: opts.time,
		size: opts.size,
		records: opts.records,
		buffer: opts.buffer,
		debug: opts.debug
	}, function(obj, done) {
		size += obj.gzipSize;
		records.push(obj);

		done(null, {
			size: obj.gzipSize,
			records: 1
		});
	}, retry.run, function flush(done) {
		logger.info("toDynamoDB On Flush");
		done();
	});
}

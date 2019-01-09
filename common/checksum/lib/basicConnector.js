"use strict";
const crypto = require('crypto');
const base = require("./handler.js");
const moment = require("moment");
const logger = require("leo-logger")("leo-checksum.basic");
const Stream = require('stream').Stream;

function promisify(method, arity) {
	if (method.length > arity) {
		return function(...args) {
			return new Promise((resolve, reject) => {
				args.push((...args) => {
					if (args[0]) reject(args[0]);
					else resolve.apply(this, args.splice(1));
				});
				method.apply(this, args);
			});
		};
	} else {
		return method;
	}
}

function readArray(array) {
	let stream = new Stream(),
		i = 0,
		paused = false,
		ended = false;

	stream.readable = true;
	stream.writable = false;

	if (!Array.isArray(array))
		throw new Error('event-stream.read expects an array');

	stream.resume = function() {
		if (ended) return;
		paused = false;
		let l = array.length;
		while (i < l && !paused && !ended) {
			stream.emit('data', array[i++]);
		}
		if (i == l && !ended)
			ended = true, stream.readable = false, stream.emit('end');
	};
	process.nextTick(stream.resume);
	stream.pause = function() {
		paused = true;
	};
	stream.destroy = function() {
		ended = true;
		stream.emit('close');
	};
	return stream;
}

function buildKeys(settings) {
	return Array.isArray(settings.fields) ? () => settings.fields : Object.keys;
}
module.exports = function(opts) {

	opts = Object.assign({
		batch: () => {
			return Promise.reject("Batch Not Implemented");
		},
		individual: () => {
			return Promise.reject("Individual Not Implemented");
		},
		sample: () => {
			return Promise.reject("Sample Not Implemented");
		},
		nibble: () => {
			return Promise.reject("Nibble Not Implemented");
		},
		range: () => {
			return Promise.reject("Range Not Implemented");
		},
		initialize: () => {
			return Promise.resolve({});
		},
		destroy: () => {
			return Promise.resolve({});
		},
		delete: () => {
			return Promise.reject("Delete Not Implemented");
		}
	}, opts);

	let wrap = opts.wrap || ((handler, base) => {
		return function(event, callback) {
			base(event, handler, callback);
		};
	});
	return base({
		batch: wrap(opts.batch, batch),
		individual: wrap(opts.individual, individual),
		sample: wrap(opts.sample, sample),
		nibble: wrap(opts.nibble, nibble),
		range: wrap(opts.range, range),
		initialize: wrap(opts.initialize, initialize),
		destroy: wrap(opts.destroy, destroy),
		delete: wrap(opts.delete, deleteFn)
	});

	function batch(event, handler, callback) {
		logger.log("Calling Batch", event);

		let startTime = moment.now();
		let data = event.data;

		promisify(handler, 2).call({
			settings: event.settings,
			session: event.session
		}, data.start, data.end).then(stream => {
			if (!Array.isArray(stream) && stream.hash) {
				return callback(null, {
					qty: stream.qty,
					ids: data.ids,
					start: data.start,
					end: data.end,
					hash: stream.hash
				});
			}

			let ObjectKeys = buildKeys(event.settings);
			stream = Array.isArray(stream) ? readArray(stream) : stream;

			let result = {
				qty: 0,
				ids: data.ids,
				start: data.start,
				end: data.end,
				hash: [0, 0, 0, 0]
			};

			stream.on("end", () => {
				result.duration = moment.now() - startTime;
				callback(null, result);
			}).on("error", (err) => {
				logger.log("Batch On Error", err);
				callback(err);
			}).on("data", (obj) => {
				let allFields = "";
				ObjectKeys(obj).forEach(key => {
					let value = obj[key];
					if (value instanceof Date) {
						allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
					} else if (value !== null && value !== undefined && value.toString) {
						allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
					} else {
						allFields += " ";
					}
				});

				let hash = crypto.createHash('md5').update(allFields).digest();

				result.hash[0] += hash.readUInt32BE(0);
				result.hash[1] += hash.readUInt32BE(4);
				result.hash[2] += hash.readUInt32BE(8);
				result.hash[3] += hash.readUInt32BE(12);
				result.qty += 1;
			});
		}).catch(callback);
	}

	function individual(event, handler, callback) {
		logger.log("Calling Individual", event);
		let startTime = moment.now();
		let data = event.data;
		promisify(handler, 2).call({
			settings: event.settings,
			session: event.session
		}, data.start, data.end).then(stream => {
			if (!Array.isArray(stream) && stream.checksums) {
				return callback(null, {
					ids: data.ids,
					start: data.start,
					end: data.end,
					qty: stream.qty,
					checksums: stream.checksums
				});
			}

			let ObjectKeys = buildKeys(event.settings);
			stream = Array.isArray(stream) ? readArray(stream) : stream;
			let results = {
				ids: data.ids,
				start: data.start,
				end: data.end,
				qty: 0,
				checksums: []
			};
			stream.on("end", () => {
				results.duration = moment.now() - startTime;
				callback(null, results);
			}).on("error", (err) => {
				logger.log("Individual On Error", err);
				callback(err);
			}).on("data", (obj) => {
				let allFields = "";

				ObjectKeys(obj).forEach(key => {
					let value = obj[key];
					if (value instanceof Date) {
						allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
					} else if (value !== null && value !== undefined && value.toString) {
						allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
					} else {
						allFields += " ";
					}
				});
				let hash = crypto.createHash('md5').update(allFields).digest('hex');
				results.checksums.push({
					id: obj[event.settings.id_column],
					_id: event.settings._id_column ? obj[event.settings._id_column] : undefined,
					hash: hash
				});
				results.qty += 1;
			});

		}).catch(callback);
	}

	function sample(event, handler, callback) {
		logger.log("Calling Sample", event);
		let data = event.data;

		promisify(handler, 1).call({
			settings: event.settings,
			session: event.session
		}, data.ids).then(stream => {
			if (!Array.isArray(stream) && stream.checksums) {
				return callback(null, {
					ids: data.ids,
					start: data.start,
					end: data.end,
					qty: stream.qty,
					checksums: stream.checksums
				});
			}

			let ObjectKeys = buildKeys(event.settings);
			stream = Array.isArray(stream) ? readArray(stream) : stream;

			let results = {
				qty: 0,
				ids: [],
				start: data.start,
				end: data.end,
				checksums: []
			};

			stream.on("end", function() {
				callback(null, results);
			}).on("err", function(err) {
				logger.log("error");
				throw err;
			}).on("data", function(obj) {
				let out = [];
				ObjectKeys(obj).forEach(key => {
					let value = obj[key];
					if (value instanceof Date) {
						out.push(Math.round(value.getTime() / 1000) + "  " + moment(value).utc().format());
					} else if (value && typeof value == "object" && value.toHexString) {
						out.push(value.toString());
					} else {
						out.push(value);
					}
				});

				results.ids.push(obj[event.settings.id_column]);
				results.checksums.push(out);
				results.qty += 1;
			});

		}).catch(callback);
	}

	function range(event, handler, callback) {
		logger.log("Calling Range", event);

		let data = event.data;
		promisify(handler, 2).call({
			settings: event.settings,
			session: event.session
		}, data.start, data.end).then(data => {
			callback(null, data);
		}).catch(callback);
	}

	function nibble(event, handler, callback) {
		logger.log("Calling Nibble", event);

		let data = event.data;
		promisify(handler, 4).call({
			settings: event.settings,
			session: event.session
		}, data.start, data.end, data.limit, data.reverse).then(data => {
			callback(null, Object.assign({
				current: null,
				next: null
			}, data));
		}).catch(callback);
	}

	function initialize(event, handler, callback) {
		logger.log("Calling Initialize", event);
		promisify(handler, 1).call({
			settings: event.settings,
			session: event.session
		}, event.data).then(data => callback(null, data)).catch(callback);
	}

	function destroy(event, handler, callback) {
		logger.log("Calling Destroy", event);
		promisify(handler, 1).call({
			settings: event.settings,
			session: event.session
		}, event.data).then(() => callback()).catch(callback);
	}

	function deleteFn(event, handler, callback) {
		logger.log("Calling Delete", event);
		promisify(handler, 1).call({
			settings: event.settings,
			session: event.session
		}, event.data.ids).then(() => callback()).catch(callback);
	}
};

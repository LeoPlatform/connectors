"use strict";
const async = require("async");
const createNibbler = require("./nibbler.js");

let logger = require("leo-logger")("leo-checksum.nibbler");

module.exports = function(local, remote, opts) {

	let nibbler = createNibbler(local, opts);
	let data = Object.assign({
		progress: 0,
		totalCorrect: 0,
		totalMissing: 0,
		totalIncorrect: 0,
		totalExtra: 0,
		streak: 0,
	}, (opts || {}).totals);
	// logger.log("START VALUES", data);

	// Add extra param to this sync;
	let sync = nibbler.sync;
	nibbler.sync = function(opts, resultCallback, callback) {
		let asyncMethod = opts.inSeries ? "mapSeries" : "map";
		let fieldNames = opts.fieldNames || [];
		let update = function(nibble, obj, results) {
			obj.progress += results.qty;
			obj.totalCorrect += results.correct;
			obj.totalMissing += results.missing;
			obj.totalIncorrect += results.incorrect;
			obj.totalExtra += results.extra;

			if (!results.missing && !results.incorrect && !results.extra) {
				obj.streak += results.qty;
			} else {
				obj.streak = 0;
			}

			nibble.progress = data.progress;
			nibbler.log(`Correct: ${results.correct}, Incorrect: ${results.incorrect}, Missing: ${results.missing}, Extra: ${results.extra}, Start: ${nibble.start}, End: ${nibble.end}`);
		};

		let compare = function(start, end, callback) {
			if (opts.skipBatch) {
				return callback(true, {
					errors: ["Skipping"],
					[local.name]: {
						hash: []
					},
					[remote.name]: {
						hash: []
					},
					duration: 0,
					qty: undefined
				});
			}
			nibbler.timeLog("Running Master & Slave Batch Checksum");
			async [asyncMethod]([local, remote], (connector, done) => {
				connector.getChecksum({
					start,
					end
				}).then(result => {
					done(null, result);
				}, callback);
			}, (err, batchResults) => {
				if (err) {
					callback(err);
					return;
				}
				let localData = batchResults[0];
				let remoteData = batchResults[1];
				let result = {
					errors: []
				};
				result[local.name] = localData;
				result[remote.name] = remoteData;

				if (localData.qty > remoteData.qty) {
					result.errors.push(remote.name + " has too few");
				}
				if (localData.qty < remoteData.qty) {
					result.errors.push(remote.name + " has too many");
				}
				if (
					localData.hash[0] != remoteData.hash[0] ||
					localData.hash[1] != remoteData.hash[1] ||
					localData.hash[2] != remoteData.hash[2] ||
					localData.hash[3] != remoteData.hash[3]
				) {
					result.errors.push("Hashes do not match");
				}

				result.duration = Math.max(localData.duration, remoteData.duration);
				result.qty = localData.qty;

				if (result.errors.length == 0) {
					callback(null, result);
				} else {
					callback(true, result);
				}
			});
		};

		let compareIndividual = function(start, end, callback) {
			// log("running individual", start, end);
			nibbler.timeLog("Running Master & Slave Individual Checksum");
			async [asyncMethod]([local, remote], (connector, done) => {
				connector.getIndividualChecksums({
					start,
					end
				}).then(result => done(null, result), callback);
			}, (err, indivData) => {
				if (err) {
					callback(err);
					return;
				}
				let localData = indivData[0];
				let remoteData = indivData[1];
				let results = {
					missing: [],
					extra: [],
					incorrect: [],
					map: {},
					qty: localData.checksums.length
				};

				let localLookup = {};
				let remoteLookup = {};
				localData.checksums.map((o, i) => {
					localLookup[o.id] = i;
				});
				remoteData.checksums.map((o, i) => {
					remoteLookup[o.id] = i;
					if (!(o.id in localLookup)) {
						if (o._id) {
							results.map[o.id] = o._id;
						}
						results.extra.push(o.id);
					}
				});
				localData.checksums.map((o, i) => {
					if (!(o.id in remoteLookup)) {
						if (o._id) {
							results.map[o.id] = o._id;
						}
						results.missing.push(o.id);
					} else if (o.hash != remoteData.checksums[remoteLookup[o.id]].hash) {
						if (o._id) {
							results.map[o.id] = o._id;
						}
						results.incorrect.push(o.id);
					}
				});
				callback(null, results);
			});
		};
		let sample = function(ids, callback) {
			if (local.sample && remote.sample) {
				nibbler.timeLog("Running Master & Slave Sample");

				async [asyncMethod]([local, remote], (connector, done) => {
					connector.sample({
						ids
					}).then(result => done(null, result), done);
				}, (err, responses) => {
					if (err) {
						callback(err);
						return;
					}
					// logger.log(JSON.stringify(responses, null, 2));
					let localData = responses[0];
					let remoteData = responses[1];
					let diffs = [];
					let paddedLocalName = local.name;
					let paddedRemoteName = remote.name;
					if (paddedRemoteName.length > paddedLocalName.length) {
						paddedLocalName = paddedLocalName + " ".repeat(paddedRemoteName.length - paddedLocalName.length);
					} else if (paddedLocalName.length > paddedRemoteName.length) {
						paddedRemoteName = paddedRemoteName + " ".repeat(paddedLocalName.length - paddedRemoteName.length);
					}
					localData.ids = localData.ids.sort();
					localData.checksums.forEach((l, i) => {
						let diff = {};
						let r = remoteData.checksums[i];
						if (!r) {
							return;
						}
						let offset = Math.max(0, l.length - fieldNames.length);
						l.forEach((v, k) => {
							if (r && v !== r[k]) {
								let name = fieldNames[k - offset] || k;
								diff[name] = {
									[paddedLocalName]: v,
									[paddedRemoteName]: r[k] || null
								};
							}
						});
						offset = Math.max(0, r.length - fieldNames.length);
						r.forEach((v, k) => {
							if (l && !(k in l)) {
								let name = fieldNames[k - offset] || k;
								diff[name] = {
									[paddedLocalName]: null,
									[paddedRemoteName]: v
								};
							}
						});
						diffs.push({
							id: localData.ids[i],
							diff
						});
					});
					diffs.map(r => {
						nibbler.normalLog(`${r.id}: ${JSON.stringify(r.diff, (k, v) => v===undefined?'!!!undefined':v,2).replace(/"!!!undefined"/g, 'undefined')}`);
					});
					if (opts.onSample) {
						opts.onSample('incorrect', diffs, () => callback());
					} else {
						callback();
					}
				});
			} else {
				callback();
			}

		};

		let until = opts.until;
		let stopReason = null;
		let nibblerOpts = {
			whilst: function(nibble) {
				let streak = (!opts.stopOnStreak || data.streak < opts.stopOnStreak) ? false : "streak";
				let other = (!until || until(nibble));
				stopReason = other || streak;
				return !streak && !other;
			},
			onInit: function(nibble) {
				nibble.progress = data.progress;
			},
			onEnd: function(err, nibble, callback) {
				logger.log(`Summary`);
				logger.log(`Correct: ${data.totalCorrect}, Incorrect:${data.totalIncorrect}, Missing:${data.totalMissing}, Extra:${data.totalExtra}`);
				callback();
			},
			onError: function(err, result, nibble, done) {
				if (err && !result) {
					return done(err);
				}
				if (nibble.limit <= 20000 || result.qty < 20000 || opts.skipBatch) { //It is small enough, we need to do individual checks
					compareIndividual(nibble.start, nibble.end, (err, dataResult) => {
						if (err) {
							done(err);
							return;
						}
						//Submit them to be resent
						if (result.qty === undefined) {
							result.qty = dataResult.qty;
						}
						resultCallback(dataResult, () => {
							//We can now move on, as we have dealt with it
							dataResult.correct = result.qty - (dataResult.incorrect.length + dataResult.missing.length);
							nibble.move();
							update(nibble, data, {
								qty: result.qty,
								correct: dataResult.correct,
								incorrect: dataResult.incorrect.length,
								missing: dataResult.missing.length,
								extra: dataResult.extra.length
							});
							if (opts.stats) {
								let d = done;
								done = function() {
									opts.stats(nibble, dataResult, data, d);
								};
							}

							if (opts.sample && dataResult.missing.length) {
								nibbler.normalLog("Missing: " + dataResult.missing.slice(0, 10).join());
							}
							if (dataResult.missing.length) {
								opts.onSample('missing', dataResult.missing);
							}
							if (opts.sample && dataResult.extra.length) {
								nibbler.normalLog("Extra: " + dataResult.extra.slice(0, 10).join());
							}
							if (dataResult.extra.length) {
								opts.onSample('extra', dataResult.extra);
							}
							if (opts.shouldDelete && dataResult.extra.length) {
								remote.delete({
									ids: dataResult.extra
								}).then(result => {
									logger.log("deleted records");
								}, err => {
									logger.log(err);
								});
							}

							if (opts.sample && dataResult.incorrect.length) {
								sample(dataResult.incorrect.slice(0).sort(() => {
									return 0.5 - Math.random();
								}).slice(0, 4), () => {
									done();
								});
							} else {
								done();
							}
						});

					});
				} else {
					if (nibble.limit <= 200000) {
						nibble.limit = 20000;
					} else {
						nibble.limit = Math.max(20000, Math.round(nibble.limit / 2));
					}
					done();
				}
			},
			onBite: function(nibble, done) {
				compare(nibble.start, nibble.end, (err, result) => {
					if (err) {
						done(err, result);
					} else { //No problem, we want to move on to the next bucket
						result.correct = result.qty;
						update(nibble, data, {
							qty: result.qty,
							correct: result.correct,
							incorrect: 0,
							missing: 0,
							extra: 0
						});

						if (opts.stats) {
							let d = done;
							done = function() {
								opts.stats(nibble, result, data, d);
							};
						}

						done(null, result);
					}
				});
			}
		};

		opts = Object.assign(opts, nibblerOpts);
		return sync(opts, function(err) {
			callback(err, data, err || stopReason || "complete");
		});
	};

	return nibbler;
};

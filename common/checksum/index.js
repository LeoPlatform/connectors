let leo = require("leo-sdk");
const aws = require("aws-sdk");
const checksum = require("./lib/checksumNibbler.js");
let dynamodb = leo.aws.dynamodb;
var cron = leo.bot;

/** These are for file connector **/
var httpsObj = require("https");
var httpObj = require("http");
var URL = require("url");
const crypto = require('crypto');

const moment = require("moment");
const uuid = require("uuid");
const ls = leo.streams;

const tableName = leo.configuration.resources.LeoCron;

function saveProgress(systemId, botId, data) {
	return new Promise((resolve, reject) => {
		dynamodb.merge(tableName, botId, {
			checksum: data,
			system: {
				id: systemId
			}
		}, function(err, result) {
			if (err) {
				reject(err);
			} else {
				resolve(data);
			}
		});
	});
}

function stats(array, other) {
	array = array || [];

	var ids = array.slice(0).sort(() => {
		return 0.5 - Math.random()
	}).slice(0, 4);

	if (ids.length < 4 && other && other.length) {
		ids = ids.concat(other.slice(-1000).slice(0).sort(() => {
			return 0.5 - Math.random()
		}).filter(id => ids.indexOf(id) == -1).slice(0, 4 - ids.length));
	}

	return {
		count: array.length,
		ids: ids
	}
}

module.exports = {
	configuration: null,
	getSession: function(systemId, botId, opts) {
		opts = Object.assign({
			restart: false
		}, opts || {});
		let emptySession = {
			master: {},
			slave: {},
			sample: {
				"missing": [],
				"incorrect": [],
				"extra": []
			},
			id: uuid.v4(),
			status: 'initializing',
			startTime: moment.now(),
			totals: {
				progress: 0,
				correct: 0,
				incorrect: 0,
				missing: 0,
				extra: 0,
				streak: 0
			}
		};
		if (opts.restart) {
			return saveProgress(systemId, botId, emptySession)
		} else {
			return new Promise((resolve, reject) => {
				console.log("Getting Session", systemId, botId);
				dynamodb.get(tableName, botId, function(err, result) {
					if (err) {
						reject(err)
					} else {
						try {
							let session = emptySession;
							if (result && result.checksum && result.checksum.restart !== true && result.checksum.status !== 'complete') {
								session = result.checksum;
							}
							resolve(session)
						} catch (err) {
							reject(err);
						}
					}
				});
			});
		}
	},
	checksum: function(system, botId, master, slave, opts) {

		return new Promise((resolve, reject) => {
			function logError(err) {
				saveProgress(system, botId, {
					status: "error",
					statusReason: err.toString()
				}).then(d => reject(err)).catch(e => reject(err));
			}
			this.getSession(system, botId, opts).then((session) => {
				console.log("Session:", session);
				let tasks = [];
				master.setSession(session);
				slave.setSession(session);

				if (session.status == 'initializing') {
					tasks.push(Promise.all([
						master.init({}),
						slave.init({})
					]).then((result) => {
						resolve(result);
					}, logError));
				}
				Promise.all(tasks).then((result) => {
					if (session.min || session.max) {
						opts.min = session.min;
						opts.max = session.max;
						opts.start = session.start || opts.start;
						opts.end = session.end || opts.end;
						opts.total = session.total;
						master.range = (opts) => {
							console.log("Using Cached Range Value");
							return Promise.resolve({
								min: opts.min,
								max: opts.max,
								total: opts.total
							});
						};
					}

					let ids = {
						missing: [],
						incorrect: [],
						extra: [],
						map: {}
					};
					let loopStart = Date.now();
					opts.until = function(nibble) {
						var lastLoopDuration = Date.now() - loopStart;
						loopStart = Date.now();
						var neededTime = lastLoopDuration * 1.33;

						console.log("Check", loopStart + neededTime >= opts.stop_at, loopStart + neededTime, opts.stop_at)
						if (loopStart + neededTime >= opts.stop_at) {
							return "Out Of Time";
						} else {
							return false;
						}
					};
					opts.stats = function(nibble, result, total, done) {
						var percent = (nibble.progress / nibble.total) * 100;
						var fixed = percent.toFixed(2);

						if (fixed == "100.00" && percent < 100) {
							fixed = 99.99;
						}

						ids.missing = ids.missing.concat(result.missing || []);
						ids.incorrect = ids.incorrect.concat(result.incorrect || []);
						ids.extra = ids.extra.concat(result.extra || []);
						if (result.map) {
							Object.keys(result.map).forEach(k => {
								ids.map[k] = result.map[k]
							});
						}

						var data = Object.assign(session, {
							endTime: null,
							lastUpdate: moment.now(),
							status: "running",
							statusReason: null,
							totals: {
								missing: total.totalMissing,
								incorrect: total.totalIncorrect,
								extra: total.totalExtra,
								correct: total.totalCorrect,
								progress: nibble.progress,
								streak: total.streak
							},
							log: {
								correct: {
									count: result.correct
								},
								missing: stats(result.missing, ids.missing),
								incorrect: stats(result.incorrect, ids.incorrect),
								extra: stats(result.extra, ids.extra),
								percent: fixed
							},
							start: nibble.start,
							end: nibble.end,
							min: nibble.min,
							max: nibble.max,
							next: nibble.next,
							total: nibble.total,
							reset: null
						});
						// console.log(JSON.stringify(data, null, 2));
						saveProgress(system, botId, data).then(result => done(null, result), done);
					};

					opts.onSample = opts.sample && function(type, diff, done) {
						session.sample[type] = diff.concat(session.sample[type]).slice(0, 4);
						if (done) {
							done();
						}
					};

					let stream;
					if (opts.queue) {
						let load = leo.load(botId, opts.queue.name, {
							useS3: true,
							debug: true
						});
						if (opts.queue.transform) {
							stream = ls.pipeline(opts.queue.transform, load);
						} else {
							stream = load;
						}
					}
					checksum(master, slave, {
						showOutput: opts.showOutput,
						totals: {
							progress: session.totals.progress,
							totalCorrect: session.totals.correct,
							totalMissing: session.totals.missing,
							totalIncorrect: session.totals.incorrect,
							totalExtra: session.totals.extra,
							streak: session.totals.streak
						}
					}).sync(opts, function(result, done) {
						if (stream) {
							if (!stream.write(result)) {
								stream.once('drain', done);
							} else {
								done();
							}
						} else {
							done();
						}
					}, function(err, data, stopReason) {
						console.log("All Done");
						console.log(data);
						console.log(stopReason);
						var status = err ? ("error") : (stopReason == "Out Of Time" ? "running" : "complete");
						var tasks = (status == "complete") ? [master, slave] : [];

						if (status == "running") {
							cron.runAgain();
						}

						stream.end((err) => {
							saveProgress(system, botId,
								Object.assign(session, {
									endTime: status != "running" ? moment.now() : null,
									lastUpdate: moment.now(),
									status: status,
									statusReason: err ? err.toString() : stopReason
								}), (err, result) => {
									async.each(tasks, (connector, done) => {
										connector.destroy({
											status: status
										}, done);
									}, (err, data) => {
										resolve();
									});
								});
						});
					});
				}, logError);
			}, logError)
		});

		return;
	},
	lambdaConnector: function(id, lambdaName, settings) {
		var region = (lambdaName.match(/arn:aws:lambda:(.*?):/) || [])[1];
		const lambdaInvoker = new aws.Lambda({
			region: region || this.configuration._meta.region,
			credentials: this.configuration ? this.configuration.credentials : null
		});
		let qualifier = null;

		let session = null;

		function invoke(method) {
			return (data) => {
				return new Promise((resolve, reject) => {
					var start = moment.now();
					lambdaInvoker.invoke({
						FunctionName: lambdaName,
						InvocationType: 'RequestResponse',
						Payload: JSON.stringify({
							params: {
								querystring: {
									method: method
								}
							},
							body: {
								data: data,
								settings: settings,
								session: session
							}
						}),
						Qualifier: qualifier
					}, function(err, data) {
						var payload = undefined;
						if (!err && data.FunctionError) {
							err = data.Payload;
						} else if (!err && data.Payload != undefined) {
							var obj = JSON.parse(data.Payload);
							if (obj.statusCode == 500) {
								err = new Error(obj.body);
							} else {
								payload = obj.response;
							}
						}
						if (err) {
							reject(err);
						} else {
							resolve(payload);
						}
					});
				});
			};
		}

		return {
			id: id,
			name: id,
			init: invoke("initialize"),
			range: invoke("range"),
			nibble: invoke("nibble"),
			getChecksum: invoke("batch"),
			getIndividualChecksums: invoke("individual"),
			destroy: invoke("destroy"),
			sample: invoke("sample"),
			delete: invoke("delete"),
			setSession: (s) => {
				session = s;
			}
		};
	},
	urlConnector: function(settings) {
		let requestSettings = settings.request;
		delete settings.request;

		var urlMethod = (method) => `${settings.url}?method=${method}`;
		if (typeof settings.url === "function") {
			urlMethod = settings.url;
		}
		var postProcessResponse = (method, postBody, data) => JSON.parse(data);
		if (settings.postProcessResponse) {
			postProcessResponse = settings.postProcessResponse;
		}

		let session = null;

		function invoke(method) {
			return (data) => {
				let http = httpObj;
				let url = urlMethod(method, data);
				if (url instanceof Error) {
					return Promise.reject(url);
				}

				if (typeof url !== "string") {
					return Promise.resolve(postProcessResponse(method, {
						data: url,
						session: session,
						settings: settings
					}, url));
				}

				if (url.match(/^https/)) {
					http = httpsObj;
				}
				return new Promise((resolve, reject) => {
					var start = moment.now();
					//console.log("URL:", url)
					let requestOptions = Object.assign(URL.parse(url), {
						method: 'POST',
						headers: {
							'Content-Type': 'application/json',
						}
					}, requestSettings);
					let postBody = {
						data: data,
						settings: settings,
						session: session
					}
					var req = http.request(requestOptions, function(res) {
						res.setEncoding('utf8');
						var data = '';
						res.on('data', function(chunk) {
							data += chunk;
						});
						res.on('end', function() {
							var payload = undefined;
							if (data) {
								let obj = postProcessResponse(method, postBody, data);
								payload = obj.response;
								if (payload === undefined) {
									payload = obj;
								}
								if (obj.session) {
									var o = {
										id: session.id,
										type: session.type
									};
									Object.assign(session, obj.session, o);
								}
							}
							resolve(payload);
						})
					});
					req.on('error', function(e) {
						console.log('problem with request: ' + e.message);
						reject(e);
					});

					if (requestOptions.method !== "GET")
						// write data to request body
						req.write(JSON.stringify(postBody));
					req.end();
				});
			};
		};

		function empty() {
			return (data) => {
				return Promise.resolve({});
			}
		}

		return {
			id: settings.id,
			name: settings.name || settings.id,
			init: settings.initialize === false ? empty() : invoke("initialize"),
			range: invoke("range"),
			nibble: invoke("nibble"),
			getChecksum: invoke("batch"),
			getIndividualChecksums: invoke("individual"),
			destroy: invoke("destroy"),
			sample: invoke("sample"),
			delete: invoke("delete"),
			setSession: (s) => {
				session = s;
			}
		};
	},
	mockConnector: function(settings) {
		return function(data, callback) {
			var start = settings.mock.min;
			var rand = Object.assign({
				batch: 10,
				single: 10,
				sample: 10
			}, settings.mock.random);

			callback(null, {
				session: {
					id: Math.random() * 10000000000
				},
				name: settings.name || settings.url,
				id: settings.id,
				destroy: function(data, done) {
					if (!done && typeof data === "function") {
						done = data;
						data = {};
					}
					done();
				},
				getChecksum: function(data, callback) {
					console.log(" BATCH", settings.name, data, rand.batch)
					callback(null, {
						qty: data.end - data.start + 1,
						hash: [1, 2, 3, Math.round(Math.random() * rand.batch)],
						duration: 1000
					})
				},
				getIndividualChecksums: function(data, callback) {
					var result = {
						qty: 0,
						start: data.start,
						end: data.end,
						checksums: []
					};

					for (let i = data.start; i <= data.end; i++) {
						result.qty++;
						result.checksums.push({
							id: i,
							hash: "1-2-3-" + (Math.round(Math.random() * rand.single))
						})
					}
					console.log(" INDIVIDUAL", settings.name, data)
					callback(null, result)
				},
				sample: function(data, callback) {
					console.log(" SAMPLE", settings.name, data);
					var result = {
						qty: 0,
						ids: [],
						start: data.start,
						end: data.end,
						checksums: []
					};

					data.ids.forEach(id => {
						result.ids.push(id);
						result.checksums.push([1, 2, Math.round(Math.random() * rand.sample), Math.round(Math.random() * rand.sample)]);
						result.qty++;
					});

					callback(null, result)
				},
				range: function(data, callback) {
					var result = {
						min: settings.mock.min,
						max: settings.mock.max,
						total: settings.mock.max - settings.mock.min + 1
					};
					console.log(" RANGE", settings.name, data, result)
					callback(null, result);
				},
				nibble: function(data, callback) {
					setTimeout(function() {
						console.log(" NIBBLE", settings.name, data);
						data.end = Math.min(data.start + data.limit - 1, settings.mock.max);
						data.next = data.start + data.limit < settings.mock.max ? data.start + data.limit : null;
						callback(null, data)
					}, settings.mock.timeout || 2000)

				}
			});
		}
	},
	fileConnector: function(id, file, settings) {
		let reverse = settings.reverse ? -1 : 1;
		let db = require(file).sort((a, b) => {
			return a[settings.id_column] > b[settings.id_column] ? 1 : -1;
		});
		let session = null;

		function invoke(method, func) {
			func = func || ((d) => {});
			return (data) => {
				return new Promise((resolve, reject) => {
					//console.log(method, data)
					try {
						let result = func(data);
						//console.log(`${id} ${method}: ${JSON.stringify(result)}`);
						resolve(result);
					} catch (err) {
						console.log(`${id} ${method} Error: ${err}`);
						reject(err);
					}
				});
			};
		}

		return {
			id: id,
			name: id,
			init: invoke("initialize", () => {
				return {};
			}),
			range: invoke("range", (data) => {
				return {
					min: db[0].id,
					max: db[db.length - 1].id,
					total: db.length
				}
			}),
			nibble: invoke("nibble", (data) => {
				let set = db.filter(a => a[settings.id_column] >= data.start && a[settings.id_column] <= data.end);
				if (reverse) {
					set = set.reverse();
				}
				data.current = set[data.limit - 1] ? set[data.limit - 1][settings.id_column] : null;
				data.next = set[data.limit] ? set[data.limit][settings.id_column] : null;
				return data;
			}),
			getChecksum: invoke("batch", (data) => {
				var result = {
					qty: 0,
					ids: data.ids,
					start: data.start,
					end: data.end,
					hash: [0, 0, 0, 0]
				};

				let set = db.filter(a => a[settings.id_column] >= data.start && a[settings.id_column] <= data.end);

				if (reverse) {
					set = set.reverse();
				}

				var extract = (obj) => {
					return settings.fields.map(f => obj[f]);
				};

				set.map(obj => {
					var allFields = "";
					extract(obj).forEach(value => {
						if (value instanceof Date) {
							allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
						} else if (value !== null && value !== undefined && value.toString) {
							allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
						} else {
							allFields += " ";
						}
					});
					var hash = crypto.createHash('md5').update(allFields).digest();

					result.hash[0] += hash.readUInt32BE(0);
					result.hash[1] += hash.readUInt32BE(4);
					result.hash[2] += hash.readUInt32BE(8);
					result.hash[3] += hash.readUInt32BE(12);
					result.qty += 1;
				});

				return result;
			}),
			getIndividualChecksums: invoke("individual", (data) => {
				let set = db.filter(a => a[settings.id_column] >= data.start && a[settings.id_column] <= data.end);
				if (reverse) {
					set = set.reverse();
				}

				var extract = (obj) => {
					return settings.fields.map(f => obj[f]);
				};

				var results = {
					ids: data.ids,
					start: data.start,
					end: data.end,
					qty: 0,
					checksums: []
				};

				set.map(obj => {
					var allFields = "";

					extract(obj).forEach(value => {
						if (value instanceof Date) {
							allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
						} else if (value !== null && value !== undefined && value.toString) {
							allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
						} else {
							allFields += " ";
						}
					});

					results.checksums.push({
						id: obj[settings.id_column],
						_id: settings._id_column ? obj[settings._id_column] : undefined,
						hash: crypto.createHash('md5').update(allFields).digest('hex')
					});
					results.qty += 1;
				});
				return results;
			}),
			destroy: invoke("destroy"),
			sample: invoke("sample", (data) => {
				let lookup = {};
				data.ids.map(i => lookup[i] = true);
				let set = db.filter(a => lookup[a[settings.id_column]]);

				var results = {
					qty: 0,
					ids: [],
					checksums: []
				};

				var extract = (obj) => {
					return settings.fields.map(f => obj[f]);
				};

				set.map(obj => {
					var out = [obj[settings.id_column]];
					extract(obj).forEach(value => {
						if (value instanceof Date) {
							out.push(Math.round(value.getTime() / 1000) + "  " + moment(value).utc().format());
						} else if (value && typeof value == "object" && value.toHexString) {
							out.push(value.toString());
						} else {
							out.push(value);
						}
					});

					results.ids.push(obj[settings.id_column]);
					results.checksums.push(out);
					results.qty += 1;
				});
				return results;

			}),
			delete: invoke("delete"),
			setSession: (s) => {
				session = s;
			}
		};
	}
};
'use strict';
const async = require('async');

const { defaultProvider } = require('@aws-sdk/credential-provider-node'); // V3 SDK.
const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');

const https = require('https');
const logger = require('leo-logger')('leo.connector.elasticsearch');
const moment = require('moment');
const refUtil = require('leo-sdk/lib/reference.js');
const leo = require('leo-sdk');
const ls = leo.streams;

const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const s3 = require('leo-aws/factory')('S3', {
	requestHandler: new NodeHttpHandler({
		httpsAgent: new https.Agent({
			keepAlive: true
		})
	}),
})._service;

/**
 *
 * @param clientConfigHost
 * @param region
 * @returns {({search} & {disconnect: m.disconnect, streamToTableFromS3: m.streamToTableFromS3, queryWithScroll: (function(*, *=): Promise<unknown>), describeTable: m.describeTable, stream: (function(*=)), streamToTableBatch: (function(*=): *), query: m.query, getIds: m.getIds, describeTables: m.describeTables, streamParallel: (function(*=)), streamToTable: (function(*=): *)}) | any}
 */
module.exports = function(clientConfigHost, region) {
	// elasticsearch client
	let m;
	let config;
	if (clientConfigHost && typeof clientConfigHost === 'object' && clientConfigHost.search) {
		m = clientConfigHost;
	} else {
		if (typeof clientConfigHost === 'string') {
			config = {
				host: clientConfigHost,
			};
		} else {
			config = clientConfigHost;
		}

		if (region) {
			config.awsConfig = {
				region,
			};
		}

		config.awsConfig = config.awsConfig || {}
		if (config.host && !config.node) {
			config.node = config.host;
		}

		let awsSigner = AwsSigv4Signer({
			region: config.awsConfig.region || process.env.AWS_REGION || 'us-east-1',
			service: 'es', // 'aoss' for OpenSearch Serverless
			// Must return a Promise that resolve to an AWS.Credentials object.
			// This function is used to acquire the credentials when the client start and
			// when the credentials are expired.
			// The Client will treat the Credentials as expired if within
			// `requestTimeout` ms of expiration (default is 30000 ms).

			// Example with AWS SDK V3:
			getCredentials: () => {
				// Any other method to acquire a new Credentials object can be used.
				const credentialsProvider = defaultProvider();
				return credentialsProvider();
			},
		});


		// Inject a wrapper that returns response.body instead of response
		// This is how the previous sdk worked so we want to preserve it
		if (!config.returnFullResponse) {
			let req = awsSigner.Transport.prototype.request;
			awsSigner.Transport.prototype.request = function(params, options = {}, callback = undefined) {
				if (callback) {
					return req.call(this, params, options, (err, response) => {
						callback(err, response ? (response.body || response) : response);
					});
				} else {
					return req.call(this, params, options).then(response => response.body || response)
				}
			};
		}

		m = new Client({
			...awsSigner,
			...config,
		});
	}

	let queryCount = 0;
	let client = Object.assign(m, {
		getIds: (queries, done) => {
			// Run any deletes and finish
			let allIds = [];
			async.eachSeries(queries, (data, callback) => {
				client.queryWithScroll({
					index: data.index,
					query: data.query,
					scroll: '15s',
					source: ['_id'],
					type: data.type,
				}, (err, ids) => {
					if (err) {
						callback(err);
						return;
					}
					allIds = allIds.concat(ids.items.map(id => id._id));
					callback();
				});
			}, (err) => {
				if (err) {
					logger.error(err);
					done(err);
				} else {
					done(null, allIds);
				}
			});
		},
		query: function(query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}
			let queryId = ++queryCount;
			let log = logger.sub('query');
			log.info(`Elasticsearch query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			m.search(query, function(err, result) {
				let fields = {};
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info(`Had error #${queryId}`, err);
				}
				callback(err, result, fields);
			});
		},
		queryWithScroll: (data, callback) => {
			const results = {
				items: [],
				qty: 0,
				scrolls: [],
				took: 0,
			};

			const max = (data.max >= 0) ? data.max : 100000;
			let transforms = {
				full: (item) => item,
				source: (item) => item._source,
			};

			let transform;
			if (typeof data.return === 'function') {
				transform = data.return;
			} else {
				transform = transforms[data.return || 'full'] || transforms.full;
			}

			let scroll = data.scroll;

			return new Promise((resolve, reject) => {
				function getUntilDone(err, data) {
					if (err) {
						logger.error(err);
						if (callback) {
							callback(err);
						} else {
							reject(err);
						}
						return;
					}
					if (data.aggregations) {
						results.aggregations = data.aggregations;
					}
					let info = data.hits;
					info.qty = info.hits.length;
					results.qty += info.qty;
					results.took += data.took;

					info.hits.forEach(item => {
						results.items.push(transform(item));
					});
					delete info.hits;

					results.total = info.total;
					results.scrolls.push(info);

					delete results.scrollid;

					if (info.qty > 0 && info.total !== results.qty) {
						results.scrollid = data._scroll_id;
					}

					if (scroll && info.total !== results.qty && max > results.qty && results.scrollid) {
						client.scroll({
							scroll,
							scrollId: data._scroll_id,
						}, getUntilDone);
					} else {
						if (callback) {
							callback(null, results);
						} else {
							resolve(results);
						}
						return;
					}
				}

				if (data.scrollid) {
					logger.debug('Starting As Scroll');
					client.scroll({
						scroll,
						scrollId: data.scrollid,
					}, getUntilDone);
				} else {
					logger.debug('Starting As Query');
					const searchObj = {
						body: {
							_source: data.source,
							aggs: data.aggs,
							from: data.from || 0, // From doesn't seem to work properly.  It appears to be ignored
							query: data.query,
							size: Math.min(max, data.size >= 0 ? data.size : 10000),
							sort: data.sort,
							track_total_hits: data.track_total_hits,
						},
						index: data.index,
						scroll,
						type: data.type,
					};
					logger.debug(JSON.stringify(searchObj, null, 2));
					client.search(searchObj, getUntilDone);
				}
			});
		},
		disconnect: () => {
		},
		describeTable: function(table, callback) {
			throw new Error('Not Implemented');
		},
		describeTables: function(callback) {
			throw new Error('Not Implemented');
		},
		stream: (settings) => {
			let requireType = settings.requireType || false;
			let total = settings.startTotal || 0;
			let fileCount = 0;
			let format = ls.through({
				highWaterMark: 16,
			}, function(event, done) {
				let data = event.payload || event;

				if (!data || !data.index || (requireType && !data.type) || !data.id) {
					logger.error('Invalid data. index, type, & id are required', JSON.stringify(data || ''));
					done('Invalid data. index, type, & id are required ' + JSON.stringify(data || ''));
					return;
				}
				deleteByQuery(this, client, event, data, done);
			}, function flush(callback) {
				logger.debug('Transform: On Flush');
				callback();
			});

			format.push = (function(self, push) {
				return function(meta, command, data) {
					if (meta == null) {
						push.call(self, null);
						return;
					}
					let result = '';
					if (command != undefined) {
						result = JSON.stringify(command) + '\n';
						if (data) {
							result += JSON.stringify(data) + '\n';
						}
					}
					push.call(self, Object.assign({}, meta, {
						payload: result,
					}));
				};
			})(format, format.push);

			let systemRef = refUtil.ref(settings.system, 'system');
			let send = ls.through({
				highWaterMark: 16,
			}, (input, done) => {
				if (input.payload && input.payload.length) {
					let meta = Object.assign({}, input);
					meta.event = systemRef.refId();
					delete meta.payload;

					total += input.payload.length;
					settings.logSummary && logger.info('ES Object Size:', input.bytes, input.payload.length, total);
					let body = input.payload.map(a => a.payload).join('');

					if (!body.length) {
						done(null, Object.assign(meta, {
							payload: {
								message: 'All deletes.  No body to run.',
							},
						}));
						return;
					}
					client.bulk({
						_source: false,
						body,
						fields: settings.fieldsUndefined ? undefined : false,
					}, function(err, data) {
						if (err || data.errors) {
							if (data && data.Message) {
								err = data.Message;
							} else if (data && data.items) {
								logger.error(data.items.filter((r) => {
									return 'error' in r.update;
								}).map(e => JSON.stringify(e, null, 2)));
								err = 'Cannot load';
							} else {
								logger.error(err);
								err = 'Cannot load';
							}
						}
						if (err) {
							logger.error(err);
						}

						let timestamp = moment();
						let rand = Math.floor(Math.random() * 1000000);
						let key = `files/elasticsearch/${(systemRef && systemRef.id) || 'unknown'}/${meta.id || 'unknown'}/${timestamp.format('YYYY/MM/DD/HH/mm/') + timestamp.valueOf()}-${++fileCount}-${rand}`;

						if (!settings.dontSaveResults) {
							logger.debug(leo.configuration.bus.s3, key);
							s3.upload({
								Body: JSON.stringify({
									body,
									response: data,
								}),
								Bucket: leo.configuration.bus.s3,
								Key: key,
							}, (uploaderr, data) => {
								done(err, Object.assign(meta, {
									payload: {
										error: err,
										file: data && data.Location,
										uploadError: uploaderr,
									},
								}));
							});
						} else {
							done(err);
						}
					});
				} else {
					done();
				}
			}, function flush(callback) {
				logger.debug('Elasticsearch Upload: On Flush');
				callback();
			});

			return ls.pipeline(format, ls.batch({
				bytes: 10485760 * 0.95, // 9.5MB
				count: 1000,
				field: 'payload',
				time: {
					milliseconds: 200,
				},
			}), send);
		},
		streamParallel: (settings) => {
			let parallelLimit = (settings.warmParallelLimit != undefined ? settings.warmParallelLimit : settings.parallelLimit) || 1;
			let requireType = settings.requireType || false;
			let total = settings.startTotal || 0;
			let startTime = Date.now();
			let duration = 0;
			let lastDuration = 0;
			let lastStartTime = Date.now();
			let lastAvg = 0;
			let fileCount = 0;
			let format = ls.through({
				highWaterMark: 16,
			}, function(event, done) {
				let data = event.payload || event;

				if (!data || !data.index || (requireType && !data.type) || data.id == undefined) {
					logger.error('Invalid data. index, type, & id are required', JSON.stringify(data || ''));
					done('Invalid data. index, type, & id are required ' + JSON.stringify(data || ''));
					return;
				}
				deleteByQuery(this, client, event, data, done);
			}, function flush(callback) {
				logger.debug('Transform: On Flush');
				callback();
			});

			format.push = (function(self, push) {
				return function(meta, command, data) {
					if (meta == null) {
						push.call(self, null);
						return;
					}
					let result = '';
					if (command != undefined) {
						result = JSON.stringify(command) + '\n';
						if (data) {
							result += JSON.stringify(data) + '\n';
						}
					}
					push.call(self, Object.assign({}, meta, {
						payload: result,
					}));
				};
			})(format, format.push);

			let systemRef = refUtil.ref(settings.system, 'system');
			let toSend = [];
			let firstStart = Date.now();

			let sendFunc = function(done) {
				let cnt = 0;
				let batchCnt = 0;
				lastDuration = 0;

				parallelLimit = settings.parallelLimit || parallelLimit;
				batchStream.updateLimits(bufferOpts);
				logger.time('es_emit');

				async.map(toSend, (input, done) => {
					let index = ++cnt + ' ';
					let meta = Object.assign({}, input);
					meta.event = systemRef.refId();
					delete meta.payload;

					settings.logSummary && logger.info(index + 'ES Object Size:', input.bytes, input.payload.length, total, (Date.now() - startTime) / total, lastAvg, Date.now() - firstStart, duration, duration / total);
					batchCnt += input.payload.length;
					total += input.payload.length;
					let body = input.payload.map(a => a.payload).join('');
					if (!body.length) {
						done(null, Object.assign(meta, {
							payload: {
								message: 'All deletes. No body to run.',
							},
						}));
						return;
					}
					logger.time(index + 'es_emit');
					logger.time(index + 'es_bulk');
					client.bulk({
						_source: false,
						body,
						fields: settings.fieldsUndefined ? undefined : false,
					}, function(err, data) {
						logger.timeEnd(index + 'es_bulk');
						logger.info(index, !err && data.took);

						if (data && data.took) {
							lastDuration = Math.max(lastDuration, data.took);
						}
						if (err || data.errors) {
							if (data && data.Message) {
								err = data.Message;
							} else if (data && data.items) {
								logger.error(data.items.filter((r) => {
									return 'error' in r.update;
								}).map(e => JSON.stringify(e, null, 2)));
								err = 'Cannot load';
							} else {
								logger.error(err);
								err = 'Cannot load';
							}
						}
						if (err) {
							logger.error(err);
						}

						let timestamp = moment();
						let rand = Math.floor(Math.random() * 1000000);
						let key = `files/elasticsearch/${(systemRef && systemRef.id) || 'unknown'}/${meta.id || 'unknown'}/${timestamp.format('YYYY/MM/DD/HH/mm/') + timestamp.valueOf()}-${++fileCount}-${rand}`;

						if (!settings.dontSaveResults) {
							logger.time(index + 'es_save');
							logger.debug(leo.configuration.bus.s3, key);
							s3.upload({
								Body: JSON.stringify({
									body,
									response: data,
								}),
								Bucket: leo.configuration.bus.s3,
								Key: key,
							}, (uploaderr, data) => {
								logger.timeEnd(index + 'es_save');
								logger.timeEnd(index + 'es_emit');
								done(err, Object.assign(meta, {
									payload: {
										error: err,
										file: data && data.Location,
										uploadError: uploaderr,
									},
								}));
							});
						} else {
							logger.timeEnd(index + 'es_emit');
							done(err, Object.assign(meta, {
								payload: {
									error: err,
								},
							}));
						}
					});
				}, (err, results) => {
					toSend = [];
					if (!err) {
						results.map(r => {
							this.push(r);
						});
					}
					duration += lastDuration;
					lastAvg = (Date.now() - lastStartTime) / batchCnt;
					lastStartTime = Date.now();
					logger.timeEnd('es_emit');
					logger.info(lastAvg);
					done && done(err);
				});
			};
			let send = ls.through({
				highWaterMark: 16,
			}, function(input, done) {
				if (input.payload && input.payload.length) {
					toSend.push(input);
					if (toSend.length >= parallelLimit) {
						sendFunc.call(this, done);
						return;
					}
				}
				done();
			},
				function flush(callback) {
					logger.debug('Elasticsearch Upload: On Flush');
					if (toSend.length) {
						sendFunc.call(this, callback);
					} else {
						callback();
					}
				});

			const bufferOpts = (typeof (settings.buffer) === 'object') ? settings.buffer : {
				records: settings.buffer,
			};

			let batchStream = ls.batch({
				bytes: bufferOpts.bytes || 10485760 * 0.95, // 9.5MB
				field: 'payload',
				records: settings.warmup || bufferOpts.records,
				time: bufferOpts.time || {
					milliseconds: 200,
				},
			});
			return ls.pipeline(format, batchStream, send);
		},
		streamToTableFromS3: function(table, opts) {
			throw new Error('Not Implemented');
		},
		streamToTableBatch: function(opts) {
			opts = Object.assign({
				records: 1000
			}, opts || {});

			return ls.bufferBackoff((obj, done) => {
				done(null, obj, 1, 1);
			}, (records, callback) => {
				logger.log('Inserting ' + records.length + ' records');
				let body = records.map(data => {
					let isDelete = (data.delete == true);
					let cmd = JSON.stringify({
						[isDelete ? 'delete' : 'update']: {
							_index: data.index || data._index,
							_type: data.type || data._type,
							_id: data.id || data._id
						}
					}) + '\n';
					let doc = !isDelete ? (JSON.stringify({
						doc: data.doc,
						doc_as_upsert: true
					}) + '\n') : '';
					return cmd + doc;
				}).join('');

				client.bulk({
					body: body,
					fields: false,
					_source: false
				}, function(err, data) {
					if (err || data.errors) {
						if (data && data.Message) {
							err = data.Message;
						} else if (data && data.items) {
							logger.error(data.items.filter((r) => {
								return 'error' in r.update;
							}).map(e => JSON.stringify(e, null, 2)));
							err = 'Cannot load';
						} else {
							logger.error(err);
							err = 'Cannot load';
						}
					}
					callback(err, []);
				});
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(opts) {
			opts = Object.assign({
				records: 1000
			}, opts || {});
			return this.streamToTableBatch(opts);
		}
	});
	return client;
};

function deleteByQuery(context, client, event, data, callback) {
	let meta = Object.assign({}, event);
	delete meta.payload;
	const deleteByQuery = [];
	if (data.delete) {
		if (!data.field || data.field === '_id') {
			context.push(meta, {
				delete: {
					_id: data.id,
					_index: data.index,
					_type: data.type,
				},
			});
		} else {
			const ids = Array.isArray(data.id) ? data.id : [data.id];
			const size = ids.length;
			const chunk = 1000;
			for (let i = 0; i < size; i += chunk) {
				deleteByQuery.push({
					index: data.index,
					query: {
						terms: {
							[data.field]: ids.slice(i, i + chunk),
						},
					},
					type: data.type,
				});
			}
		}
	} else {
		context.push(meta, {
			update: {
				_id: data.id,
				_index: data.index,
				_type: data.type,
			},
		}, {
			doc: data.doc,
			doc_as_upsert: true,
		});
	}

	if (deleteByQuery.length) {
		client.getIds(deleteByQuery, (err, ids) => {
			if (err) {
				callback(err);
			} else {
				ids.forEach(id => context.push(meta, {
					delete: {
						_id: id,
						_index: data.index,
						_type: data.type,
					},
				}));
				context.push(meta);
				callback();
			}
		});
	} else {
		callback();
	}
}

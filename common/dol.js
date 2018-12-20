'use strict';

const leo = require("leo-sdk");
const ls = leo.streams;
const async = require("async");
const logger = require('leo-logger');

module.exports = class Dol {
	constructor(client) {
		this.client = client;
	}

	translateIds(translations, opts) {
		opts = Object.assign({
			count: 1000,
			time: {
				milliseconds: 200
			}
		}, opts);
		return ls.pipeline(
			this.translateIdsStartStream(translations),

			ls.batch({
				count: opts.count,
				time: opts.time
			}),

			this.translateIdsLookupStream(translations),

			ls.batch({
				count: 1000,
				time: {
					milliseconds: 200
				}
			}),
			this.translateIdsCombineStream()
		);
	}

	domainObjectTransform(domainObject) {
		if (typeof domainObject.get === "function") {
			domainObject = domainObject.get();
		}
		domainObject = Object.assign({
			domainIdColumn: "_domain_id",
			query: "select * from dual limit 1",
			joins: {}
		}, domainObject);

		domainObject.sql = this.queryToFunction(domainObject.sql || domainObject.query, ["data"]);
		Object.values(domainObject.joins).map(v => {
			v.sql = this.queryToFunction(v.sql || v.query, ["data"]);
		});

		//We can jump in at this point if we just have a join table to use without going through the above...otherwise it assumes we have a list of ids
		//Lets do actual Domain lookups
		return ls.through((obj, done, push) => {
			let addCorrelation = (result, final = false, units) => {
				if (final) {
					result.correlation_id = {
						source: obj.correlation_id.source,
						start: obj.correlation_id.start || obj.correlation_id.end,
						end: obj.correlation_id.end,
						units: units
					};
				} else {
					result.correlation_id = {
						source: obj.correlation_id.source,
						partial_start: obj.correlation_id.start || obj.correlation_id.end || obj.correlation_id.partial,
						partial_end: obj.correlation_id.start ? (obj.correlation_id.partial || obj.correlation_id.end) : obj.correlation_id.end,
						units: units
					};
				}
				push(result);
			};

			if (obj.joinTable) {
				done("joinTable is not implemented");
			} else if (obj.ids) {
				//Query for these domain Objects

				this.buildDomainObject(domainObject, obj.ids, addCorrelation, (err, results = []) => {
					if (err) {
						logger.error(JSON.stringify(obj, null, 2));
						return done(err);
					}
					if (results.length) {
						let i = 0;
						for (; i < results.length - 1; i++) {
							let result = results[i];
							push(result);
						}

						let lastResult = results[i];
						addCorrelation(lastResult, true);
						addCorrelation({
							dont_write: true,
							payload: {}
						}, true, -1);
					} else {
						addCorrelation({
							dont_write: true,
							payload: {}
						}, true);
						addCorrelation({
							dont_write: true,
							payload: {}
						}, true, -1);
					}
					done();
				});

			} else {
				addCorrelation({
					dont_write: true,
					payload: {}
				}, true);
				done();
			}
		});
	}

	domainObject(query, domainIdColumn = "_domain_id", transform) {
		if (typeof domainIdColumn === "function") {
			transform = domainIdColumn;
			domainIdColumn = "_domain_id";
		}
		this.query = query;
		this.domainIdColumn = domainIdColumn;
		this.transform = transform;
		this.joins = {};

		return this;
	}

	hasMany(name, query, domainIdColumn = "_domain_id", transform) {
		if (typeof query === "object") {
			this.joins[name] = query;
			return;
		}

		if (typeof domainIdColumn === "function") {
			transform = domainIdColumn;
			domainIdColumn = "_domain_id";
		}

		this.joins[name] = {
			query,
			domainIdColumn,
			transform,
			type: "one_to_many"
		};
		return this;
	}

	/**
	 * Translate id's from a database listener
	 * formats:
	 * mysql: payload.update.database.table.ids
	 *      payload.delete.database.table.ids
	 * other databases: payload.table.ids - converted to payload.update.__database__.table.ids
	 * @param idTranslation
	 */
	translateIdsStartStream(idTranslation) {

		return ls.through((obj, done, push) => {
			if (!obj.payload.update) {
				// handle data from listeners other than mysql
				if (obj.payload && !obj.payload.delete && Object.keys(obj.payload).length) {
					obj.payload = {
						update: {
							// add a dummy database, which will be removed on the domain object final output
							__database__: obj.payload
						}
					};
				} else {
					return done(null, {
						correlation_id: {
							source: obj.event,
							start: obj.eid
						}
					});
				}
			}

			let last = null;
			let count = 0;
			let updates = obj.payload.update;
			for (let schema in updates) {
				for (let t in idTranslation) {
					let ids = updates[schema][t];
					if (!ids) {
						continue;
					}

					ids = Array.from(new Set(ids)); // Dedup the ids
					for (let i = 0; i < ids.length; i++) {
						if (count) push(last);
						last = {
							s: schema,
							t,
							id: ids[i],
							correlation_id: {
								source: obj.event,
								partial: obj.eid,
								units: 1
							}
						};
						count++;
					}
				}
			}

			if (last) {
				last.correlation_id = {
					source: obj.event,
					start: obj.eid,
					units: 1
				};
				done(null, last);
			} else {
				done(null, {
					correlation_id: {
						source: obj.event,
						start: obj.eid,
						units: 1
					}
				});
			}
		});
	}

	/**
	 * Translate id's
	 * Examples:
	 * var translateIds = {
	 * 	just_id: true, // boolean. Just uses a single primary key.
	 * 	people: ['pk1', 'pk2', etc…], // array of keys
	 * 	vehicles: { // object
	 * 		keys: ['pk1', 'pk2', etc…], // array of keys
	 * 		translation: c => { // can be a string or a function. So next 2 examples below for each type.
	 * 			// do translate stuffs
	 * 		}
	 * 	},
	 * 	regular_translation: c => { // function
	 * 		// do translation stuffs
	 * 	},
	 * 	string_translation: `SELECT * FROM (VALUES(?)) WHERE… etc…` // string
	 * };
	 * @param idTranslation
	 */
	translateIdsLookupStream(idTranslation) {
		let handlers = {};
		Object.keys(idTranslation).map(v => {
			let translation = idTranslation[v];
			if (translation === true) {
				handlers[v] = this.handleTranslateBoolean();
			} else if (typeof translation === "string" || (typeof translation === "function" && translation.length <= 1)) {
				handlers[v] = this.handleTranslateString(translation);
			} else if (typeof translation === "function") {
				handlers[v] = this.handleTranslateFunction(translation);
			} else if (Array.isArray(translation)) {
				handlers[v] = this.handleTranslateArray(translation);
			} else if (typeof translation === 'object') {
				handlers[v] = this.handleTranslateObject(translation);
			}
		});

		return ls.through((obj, done, push) => {
			let ids = {};
			let p = obj.payload;
			let startEid;
			let endEid;
			let partialEid;

			for (let i = 0; i < p.length; i++) {
				let record = p[i];
				if (record.id) {
					if (!(record.s in ids)) {
						ids[record.s] = Object.keys(idTranslation).reduce((acc, v) => (acc[v] = new Set()) && acc, {});
					}
					ids[record.s][record.t].add(record.id);
				}

				if (record.correlation_id.start) {
					if (!startEid) {
						startEid = record.correlation_id.start;
					}
					partialEid = record.correlation_id.partial;
					endEid = record.correlation_id.start || endEid;
				} else if (record.correlation_id.partial) {
					partialEid = record.correlation_id.partial;
				}
			}

			let tasks = [];
			let domainIds = [];
			Object.keys(ids).forEach(schema => {
				tasks.push(done => {
					let subTasks = [];
					Object.keys(ids[schema]).forEach(t => {
						let lookupIds = Array.from(ids[schema][t]);
						if (lookupIds && lookupIds.length) {
							let handler = handlers[t] || ((ids, d) => d());

							subTasks.push(done => {
								let context = {
									database: schema,
									schema: schema,
									table: t,
									client: this.client,
									ids: lookupIds,
									done: done
								};

								handler.call(context, context, (err, ids = []) => {
									if (err) {
										done(err);
									} else {
										ids.map(id => {
											domainIds.push({
												s: schema,
												id: id
											});

										});
										done();
									}
								});
							});
						}
					});
					async.parallelLimit(subTasks, 10, (err) => {
						done(err);
					});
				});
			});
			async.parallelLimit(tasks, 4, (err) => {
				if (err) {
					return done(err);
				} else if (!domainIds.length) {

					// TODO: should we set lastFullEid?
					return done(null, {
						correlation_id: {
							source: obj.correlation_id.source,
							start: startEid,
							end: endEid,
							partial: partialEid,
							units: p.length
						}
					});
				}
				let i = 0;
				for (; i < domainIds.length - 1; i++) {
					domainIds[i].correlation_id = {
						source: obj.correlation_id.source,
						start: startEid,
						end: endEid,
						partial: partialEid,
						units: p.length
					};
					push(domainIds[i]);
				}

				domainIds[i].correlation_id = {
					source: obj.correlation_id.source,
					start: startEid,
					end: endEid,
					partial: partialEid,
					units: p.length
				};
				done(null, domainIds[i]);
			});
		});
	}

	translateIdsCombineStream() {
		return ls.through((obj, done) => {
			let startEid;
			let endEid;
			let partialEid;

			let ids = {};
			for (let i = 0; i < obj.payload.length; i++) {
				let p = obj.payload[i];
				if (p.s !== undefined && p.id !== undefined) {
					if (!(p.s in ids)) {
						ids[p.s] = new Set();
					}
					ids[p.s].add(p.id);
				}

				let record = p;
				if (record.correlation_id.start) {
					if (!startEid) {
						startEid = record.correlation_id.start;
					}
					partialEid = record.correlation_id.partial;
					endEid = record.correlation_id.start || endEid;
				} else if (record.correlation_id.partial) {
					partialEid = record.correlation_id.partial;
				}
			}
			Object.keys(ids).map(k => {
				ids[k] = Array.from(ids[k]);
			});

			done(null, {
				ids: ids,
				correlation_id: {
					source: obj.correlation_id.source,
					start: startEid,
					end: endEid,
					partial: partialEid,
					units: obj.payload.length
				}
			});
		});
	}

	buildDomainObject(domainObject, ids, push, callback) {
		let opts = {};
		let sqlClient = this.client;

		async.eachLimit(Object.entries(ids), 5, ([schema, ids], callback) => {
			let tasks = [];
			let domains = {};
			let queryIds = [];
			logger.log(`Processing ${ids.length} from ${schema}`);
			ids.forEach(id => {

				// if the id is an object, it's a composite key
				if (typeof id === 'object') {
					// change the id to be a string with keys separated by a dash
					queryIds.push(Object.values(id));
					id = Object.values(id).join('-');
				} else {
					queryIds.push(id);
				}

				domains[id] = {};
				Object.keys(domainObject.joins).forEach(name => {
					domains[id][name] = [];
				});
			});


			tasks.push(done => {
				if (!domainObject.domainIdColumn) {
					logger.error('[FATAL ERROR]: No ID specified');
				}

				let query = domainObject.sql.call({
					database: schema,
					schema: schema,
					client: sqlClient
				}, {
					ids: queryIds,
					database: schema,
					schema: schema,
					client: sqlClient
				});

				this.buildDomainQuery(domainObject, domains, query, [queryIds], done);
			});

			Object.keys(domainObject.joins).forEach(name => {
				let t = domainObject.joins[name];
				t.domainIdColumn = t.domainIdColumn || "_domain_id";
				let query = t.sql.call({
					database: schema,
					schema: schema,
					client: sqlClient
				}, {
					ids: queryIds,
					database: schema,
					schema: schema,
					client: sqlClient
				});
				tasks.push(done => {
					this.buildJoinQuery(t, name, domains, query, [queryIds], done);
				});
			});

			this.processTasks(ids, domains, tasks, domainObject, schema, opts, push, callback);
		}, (err) => {
			callback(err);
		});
	}

	mapResults(results, fields, each) {
		let mappings = [];

		let last = null;
		fields.forEach((f, i) => {
			if (last == null) {
				last = {
					path: null,
					start: i
				};
				mappings.push(last);
			} else if (f.name.match(/^prefix_/)) {
				last.end = i;
				last = {
					path: f.name.replace(/^prefix_/, ''),
					start: i + 1
				};
				mappings.push(last);
			}
		});
		last.end = fields.length;
		results.forEach(r => {
			//Convert back to object now
			let row = {};
			mappings.forEach(m => {
				if (m.path === null) {
					r.slice(m.start, m.end).forEach((value, i) => {
						row[fields[m.start + i].name] = value;
					});
				} else if (m.path) {
					row[m.path] = r.slice(m.start, m.end).reduce((acc, value, i) => {
						acc[fields[m.start + i].name] = value;
						return acc;
					}, {});
				}
			});
			each(row);
		});
	}

	processDomainQuery({transform = a => a, domainIdColumn}, domains, done, err, results, fields) {
		if (err) return done(err);

		this.mapResults(results, fields, row => {
			let domainId = Array.isArray(domainIdColumn) ? domainIdColumn.map(i => row[i]).join('-') : row[domainIdColumn];
			row = transform(row);
			delete row._domain_id;

			if (!domainId) {
				logger.error('ID: "' + domainIdColumn + '" not found in object:');
			} else if (!domains[domainId]) {
				logger.error('ID: "' + domainIdColumn + '" with a value of: "' + domainId + '" does not match any ID in the domain object. This could be caused by using a WHERE clause on an ID that differs from the SELECT ID');
			} else {
				//We need to keep the domain relationships in tact
				domains[domainId] = Object.assign(domains[domainId], row);
			}
		});
		done();
	}

	buildDomainQuery(domainObject, domains, query, queryIds, done) {
		this.client.query(query, queryIds, (err, results, fields) => {
			this.processDomainQuery(domainObject, domains, done, err, results, fields);
		}, {
			inRowMode: true
		});
	}

	processJoinQuery({transform = a => a, domainIdColumn}, name, domains, done, err, results, fields) {
		if (err) {
			return done(err);
		} else if (!results.length) {
			return done();
		}

		this.mapResults(results, fields, row => {
			let domainId = Array.isArray(domainIdColumn) ? domainIdColumn.map(i => row[i]).join('-') : row[domainIdColumn];
			delete row._domain_id;
			row = transform(row);
			domains[domainId][name].push(row);
		});
		done();
	}

	buildJoinQuery(joinObject, name, domains, query, queryIds, done) {
		this.client.query(query, queryIds, (err, results, fields) => {
			this.processJoinQuery(joinObject, name, domains, done, err, results, fields);
		}, {
			inRowMode: true
		});
	}

	/**
	 * Process the tasks
	 * @param ids
	 * @param domains Array
	 * @param tasks
	 * @param domainObject
	 * @param schema
	 * @param opts
	 * @param push
	 * @param callback
	 */
	processTasks(ids, domains, tasks, domainObject, schema, opts, push, callback) {
		let limit = 5;
		let joinsCount = Object.keys(domainObject.joins).length;

		async.parallelLimit(tasks, limit, (err) => {
			if (err) {
				callback(err);
			} else {
				ids.forEach(id => {
					// if the id is an object, it's a composite key
					if (typeof id === 'object') {
						// change the id to be a string with keys separated by a dash
						id = Object.values(id).join('-');
					}

					// skip the domain if there is no data with it
					let keyCount = Object.keys(domains[id]).length;
					if (keyCount === 0) {
						logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
						return;
					} else if (keyCount <= joinsCount) {
						let valid = Object.keys(domainObject.joins).some(k => {
							return Object.keys(domains[id][k] || []).length > 0;
						});
						if (!valid) {
							logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
							return;
						}
					}
					// let eids = {}; // TODO: get event info
					// let eid = getEid(id, domains[id], eids);
					let event = {
						event: opts.queue,
						id: opts.id,
						payload: domains[id],
						schema: schema,
						domain_id: id
					};

					// remove the schema if it's a dummy we added in translate ids
					if (event.schema === '__database__') {
						delete event.schema;
					}

					push(event);
				});

				callback();
			}
		});
	}

	queryToFunction(query, params) {
		if (typeof query === "function") {
			return query;
		} else if (typeof query === "string") {
			params.push(`return \`${query}\`;`);
			return Function.apply(this, params);
		}
	}

	handleTranslateBoolean() {
		return (data, done) => {
			done(null, data.ids);
		};
	}

	handleTranslateArray(translation) {
		// this should be an array of primary keys for ordering purposes
		return (data, done) => {
			let ids = data.ids.map(ids => {
				let returnObj = {};

				translation.forEach(key => {
					returnObj[key] = ids[key];
				});

				return returnObj;
			});

			done(null, ids);
		};
	}

	handleTranslateObject(translation) {
		// this expects that we have a translation.translation, which is a function
		let queryFn = this.queryToFunction(translation.translation, ['data']);
		return function (data, done) {
			let query = queryFn.call(this, data);
			let ids = data.ids;

			// sort the ids
			if (translation.keys) {
				ids = data.ids.map(ids => {
					let returnObj = {};

					translation.keys.forEach(key => {
						returnObj[key] = ids[key];
					});

					return returnObj;
				});
			}

			this.client.query(query, [ids], (err, rows) => {
				done(err, rows && rows.map(r => r[0]));
			}, {
				inRowMode: true
			});
		};
	}

	handleTranslateFunction(translation) {
		return translation;
	}

	handleTranslateString(translation) {
		let queryFn = this.queryToFunction(translation, ["data"]);
		return function (data, done) {
			let query = queryFn.call(this, data);
			this.client.query(query, [data.ids], (err, rows) => {
				done(err, rows && rows.map(r => r[0]));
			}, {
				inRowMode: true
			});
		};
	}
};

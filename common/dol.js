const leo = require("leo-sdk");
const ls = leo.streams;
const async = require("async");

module.exports = function domainObjectLoader(client) {

	let obj = {
		translateIds: function (translations, opts) {
			opts = Object.assign({
				count: 1000,
				time: {
					milliseconds: 200
				}
			}, opts);
			return ls.pipeline(
				translateIdsStartStream(translations),

				ls.batch({
					count: opts.count,
					time: opts.time
				}),

				translateIdsLookupStream(client, translations),
				ls.batch({
					count: 1000,
					time: {
						milliseconds: 200
					}
				}),
				translateIdsCombineStream()
			);
		},
		domainObjectTransform: function (domainObject) {
			if (typeof domainObject.get === "function") {
				domainObject = domainObject.get();
			}
			domainObject = Object.assign({
				domainIdColumn: "_domain_id",
				query: "select * from dual limit 1",
				joins: {}
			}, domainObject);

			domainObject.sql = queryToFunction(domainObject.sql || domainObject.query, ["data"]);
			Object.values(domainObject.joins).map(v => {
				v.sql = queryToFunction(v.sql || v.query, ["data"]);
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

					buildDomainObject(client, domainObject, obj.ids, addCorrelation, (err, results = []) => {
						if (err) {
							console.log(JSON.stringify(obj, null, 2));
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
		},
		domainObject: function (query, domainIdColumn = "_domain_id", transform) {
			if (typeof domainIdColumn === "function") {
				transform = domainIdColumn;
				domainIdColumn = "_domain_id";
			}
			this.query = query;
			this.domainIdColumn = domainIdColumn;
			this.transform = transform;
			this.joins = {};
			this.hasMany = function (name, query, domainIdColumn = "_domain_id", transform) {
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
			};
		},
	};

	// create an alias for existing implementations
	obj.DomainObject = obj.domainObject;

	return obj;
};

/**
 * Translate id's from a database listener
 * formats:
 * mysql: payload.update.database.table.ids
 *      payload.delete.database.table.ids
 * other databases: payload.table.ids - converted to payload.update.__database__.table.ids
 * @param idTranslation
 */
function translateIdsStartStream(idTranslation) {

	return ls.through((obj, done, push) => {
		if (!obj.payload.update) {
			// handle data from listeners other than mysql
			if (obj.payload && !obj.payload.delete && Object.keys(obj.payload).length) {
				obj.payload.update = {
					// add a dummy database, which will be removed on the domain object final output
					__database__: obj.payload
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

				ids = Array.from(new Set(ids)); // Dedub the ids
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

function translateIdsLookupStream(client, idTranslation) {
	let handlers = {};
	Object.keys(idTranslation).map(v => {
		let translation = idTranslation[v];
		if (translation === true) {
			handlers[v] = (data, done) => {
				done(null, data.ids);
			};
		} else if (typeof translation === "string" || (typeof translation === "function" && translation.length <= 1)) {
			let queryFn = queryToFunction(translation, ["data"]);
			handlers[v] = function (data, done) {
				let query = queryFn.call(this, data);
				this.client.query(query, [data.ids], (err, rows) => {
					done(err, rows && rows.map(r => r[0]));
				}, {
					inRowMode: true
				});
			};
		} else if (typeof translation === "function") {
			handlers[v] = translation;
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
								client: client,
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

function translateIdsCombineStream() {
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

function buildDomainObject(client, domainObject, ids, push, callback) {
	let opts = {};
	let sqlClient = client;
	let joinsCount = Object.keys(domainObject.joins).length;

	async.eachLimit(Object.entries(ids), 5, ([schema, ids], callback) => {
		let tasks = [];
		let domains = {};
		console.log(`Processing ${ids.length} from ${schema}`);
		ids.forEach(id => {
			domains[id] = {};
			Object.keys(domainObject.joins).forEach(name => {
				let t = domainObject.joins[name];
				if (t.type === "one_to_many") {
					domains[id][name] = [];
				} else {
					domains[id][name] = {};
				}
			});
		});

		function mapResults(results, fields, each) {
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

		tasks.push(done => {
			if (!domainObject.domainIdColumn) {
				console.log('[FATAL ERROR]: No ID specified');
			}

			let query = domainObject.sql.call({
				database: schema,
				schema: schema,
				client: sqlClient
			}, {
				ids: ids,
				database: schema,
				schema: schema,
				client: sqlClient
			});
			sqlClient.query(query, [ids], (err, results, fields) => {
				if (err) return done(err);

				mapResults(results, fields, row => {
					let domainId = row[domainObject.domainIdColumn];
					if (domainObject.transform) {
						row = domainObject.transform(row);
					}
					delete row._domain_id;

					if (!domainId) {
						console.error('ID: "' + domainObject.domainIdColumn + '" not found in object:');
					} else if (!domains[domainId]) {
						console.error('ID: "' + domainObject.domainIdColumn + '" with a value of: "' + domainId + '" does not match any ID in the domain object. This could be caused by using a WHERE clause on an ID that differs from the SELECT ID');
					} else {
						//We need to keep the domain relationships in tact
						domains[domainId] = Object.assign(domains[domainId], row);
					}
				});
				done();
			}, {
				inRowMode: true
			});
		});

		Object.keys(domainObject.joins).forEach(name => {
			let t = domainObject.joins[name];
			t.domainIdColumn = t.domainIdColumn || "_domain_id";
			let query = t.sql.call({
				database: schema,
				schema: schema,
				client: sqlClient
			}, {
				ids: ids,
				database: schema,
				schema: schema,
				client: sqlClient
			});
			if (t.type === "one_to_many") {
				tasks.push(done => {
					sqlClient.query(query, [ids], (err, results, fields) => {
						if (err) {
							return done(err);
						} else if (!results.length) {
							return done();
						}

						mapResults(results, fields, row => {
							let domainId = row[t.domainIdColumn];
							delete row._domain_id;

							if (t.transform) {
								row = t.transform(row);
							}

							domains[domainId][name].push(row);
						});
						done();
					}, {
						inRowMode: true
					});
				});
			} else {
				tasks.push(done => {
					sqlClient.query(query, [ids], (err, results, fields) => {
						if (err) {
							return done(err);
						} else if (!results.length) {
							return done();
						}

						mapResults(results, fields, row => {
							if (row.length) {
								let domainId = row[t.domainIdColumn];
								delete row._domain_id;
								if (t.transform) {
									row = t.transform(row);
								}
								domains[domainId][name] = row;
							}
						});
						done();
					}, {
						inRowMode: true
					});
				});
			}
		});
		let limit = 5;
		async.parallelLimit(tasks, limit, (err) => {
			if (err) {
				callback(err);
			} else {
				ids.forEach(id => {
					// skip the domain if there is no data with it
					let keyCount = Object.keys(domains[id]).length;
					if (keyCount === 0) {
						console.log('[INFO] Skipping domain id due to empty object. #: ' + id);
						return;
					} else if (keyCount <= joinsCount) {
						let valid = Object.keys(domainObject.joins).some(k => {
							return Object.keys(domains[id][k] || []).length > 0;
						});
						if (!valid) {
							console.log('[INFO] Skipping domain id due to empty object. #: ' + id);
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
	}, (err) => {
		callback(err);
	});
}

function queryToFunction(query, params) {
	if (typeof query === "function") {
		return query;
	} else if (typeof query === "string") {
		params.push(`return \`${query}\`;`);
		return Function.apply(this, params);
	}
}

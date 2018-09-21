const leo = require("leo-sdk");
const ls = leo.streams;
const async = require("async");

module.exports = function DomainObjectLoader(client) {
	let self = this;
	return {
		translateIds: function(translations, opts) {
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
		domainObjectTransform: function(domainObject) {
			if (typeof domainObject.get == "function") {
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
		DomainObject: function(query, domainIdColumn = "_domain_id", transform) {
			if (typeof domainIdColumn === "function") {
				transform = domainIdColumn;
				domainIdColumn = "_domain_id";
			}
			this.query = query;
			this.domainIdColumn = domainIdColumn;
			this.transform = transform;
			this.joins = {};
			this.hasMany = function(name, query, domainIdColumn = "_domain_id", transform) {
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
			},
			this.transfrom = () => self.domainObjectTransform(this);
		}
	};
};



function translateIdsStartStream(idTranslation) {
	// let bufferIds = {};
	// let lastFullEid = null;

	return ls.through((obj, done, push) => {
		if (!obj.payload.update) {
			return done(null, {
				correlation_id: {
					source: obj.event,
					start: obj.eid
				}
			});
		}
		let last = null;
		let count = 0;
		let updates = obj.payload.update;
		for (let schema in updates) {
			for (var t in idTranslation) {
				let ids = updates[schema][t];
				if (!ids) {
					continue;
				}

				ids = Array.from(new Set(ids)); // Dedub the ids
				for (var i = 0; i < ids.length; i++) {
					if (count) push(last);
					last = {
						s: schema,
						t,
						id: ids[i],
						correlation_id: {
							source: obj.event,
							//start: lastFullEid
							partial: obj.eid,
							units: 1
						}
					};
					count++;
				}
			}
		}

		// lastFullEid = obj.eid;
		if (last) {
			count++;
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
	// let lastFullEid = null;
	let handlers = {};
	Object.keys(idTranslation).map(v => {
		let translation = idTranslation[v];
		if (translation === true) {
			handlers[v] = (data, done) => {
				done(null, data.ids);
			};
		} else if (typeof translation === "string" || (typeof translation === "function" && translation.length <= 1)) {
			let queryFn = queryToFunction(translation, ["data"]);
			handlers[v] = function(data, done) {
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

			//console.log((record.correlation_id.start ? "start" : ""), record.correlation_id.partial ? "partial" : "", ":", record.correlation_id.start, record.correlation_id.partial);
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
						//start: obj.correlation_id.end || obj.correlation_id.start,
						start: startEid,
						end: endEid,
						partial: partialEid,
						units: p.length
						//units: obj.correlation_id.units
					}
				});
			}
			let i = 0;
			for (; i < domainIds.length - 1; i++) {
				domainIds[i].correlation_id = {
					source: obj.correlation_id.source,
					// start: lastFullEid,
					// units: obj.correlation_id.units
					start: startEid,
					end: endEid,
					partial: partialEid,
					units: p.length
				};
				push(domainIds[i]);
			}
			// lastFullEid = obj.correlation_id.end || obj.correlation_id.start;
			domainIds[i].correlation_id = {
				source: obj.correlation_id.source,
				//start: lastFullEid,
				//units: obj.correlation_id.units || 1

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
		//let units = 0;

		let ids = {};
		for (var i = 0; i < obj.payload.length; i++) {
			let p = obj.payload[i];
			if (!(p.s in ids)) {
				ids[p.s] = new Set();
			}
			ids[p.s].add(p.id);

			let record = p;
			if (record.correlation_id.start) {
				if (!startEid) {
					startEid = record.correlation_id.start;
				}
				partialEid = record.correlation_id.partial;
				endEid = record.correlation_id.start || endEid;
				//units += (record.correlation_id.units || record.correlation_id.records || 1);
			} else if (record.correlation_id.partial) {
				partialEid = record.correlation_id.partial;
			}

			//console.log("combine", (record.correlation_id.start ? "start" : ""), record.correlation_id.partial ? "partial" : "", ":", record.correlation_id.start, record.correlation_id.partial);
		}
		Object.keys(ids).map(k => {
			ids[k] = Array.from(ids[k]);
		});

		done(null, {
			ids: ids,
			correlation_id: {
				source: obj.correlation_id.source,
				//start: obj.correlation_id.start || obj.correlation_id.end,
				//units: obj.correlation_id.units
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
					//row._schema = schema;
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
				// let getEid = opts.getEid || ((id, obj, stats) => stats.end);
				ids.forEach(id => {
					// skip the domain if there is no data with it
					if (Object.keys(domains[id]).length === 0) {
						console.log('[INFO] Skipping domain id due to empty object. #: ' + id);
						return;
					}
					// let eids = {}; // TODO: get event info
					// let eid = getEid(id, domains[id], eids);
					let event = {
						event: opts.queue,
						id: opts.id,
						payload: domains[id],
						schema: schema
					};
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

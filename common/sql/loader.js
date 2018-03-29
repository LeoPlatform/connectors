"use strict";
const logger = require("leo-sdk/lib/logger")("leo.connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const leo = require("leo-sdk");
const ls = leo.streams;

const builder = require("./loaderBuilder.js");

module.exports = function(sqlClient, sql, domainObj, opts) {
	const MAX = 5000;
	let ids = [];
	opts = Object.assign({
		source: "loader",
		isSnapshot: false
	}, opts || {});

	function submit(push, done) {
		async.doWhilst((done) => {
			let buildIds = ids.splice(0, MAX);
			buildEntities(buildIds, push, done);
		}, () => ids.length >= MAX, (err) => {
			if (err) {
				done(err);
			} else {
				done();
			}
		});
	}

	let eids = {
		start: null,
		end: null
	};
	return ls.through({
		highWaterMark: opts.isSnapshot ? 2 : 16
	}, (obj, done, push) => {
		eids.start = eids.start || obj.eid;
		eids.end = obj.eid;
		let tasks = [];
		let findIds = [];

		if (obj.jointable) {
			submit(push, done);
		} else if (typeof sql == "function") {
			function processIdList(idlist) {
				idlist.forEach(idthing => {
					if (Array.isArray(idthing)) {
						findIds = findIds.concat(idthing);
					} else if (typeof idthing == "string") {
						tasks.push((done) => {
							sqlClient.query(idthing, (err, results, fields) => {
								if (!err) {
									let firstColumn = fields[0].name;
									findIds = findIds.concat(results.map(row => row[firstColumn]));
								}
								done(err);
							});
						});
					}
				});
				async.parallelLimit(tasks, 10, (err, results) => {
					if (err) {
						done(err);
					} else {
						ids = ids.concat(findIds.filter((e, i, self) => {
							return ids.indexOf(e) === -1 && self.indexOf(e) === i;
						}));
						if (ids.length >= MAX) {
							submit(push, done);
						} else {
							done();
						}
					}
				});
			}
			let handledReturn = false;
			let result = sql.call({
				hasIds: (a) => {
					if (a && Array.isArray(a) && a.length) {
						return true;
					} else {
						return false;
					}
				}
			}, obj.payload, (err, idlist) => {
				if (!handledReturn) {
					if (err) return done(err);
					processIdList(idlist);
				}
			});
			if (result && Array.isArray(result)) {
				handledReturn = true;
				processIdList(result);
			}
		} else {
			Object.keys(sql).forEach(key => {
				if (sql[key] === true && key in obj.payload) {
					findIds = findIds.concat(obj.payload[key]);
				} else if (key in obj.payload && obj.payload[key].length) {
					tasks.push((done) => {
						logger.debug(obj.payload, key);
						let s = sql[key].replace(/__IDS__/, obj.payload[key].join());
						sqlClient.query(s, (err, results, fields) => {
							if (!err) {
								let firstColumn = fields[0].name;
								findIds = findIds.concat(results.map(row => row[firstColumn]));
							}
							done(err);
						});
					});
				}
			});
			async.parallelLimit(tasks, 10, (err, results) => {
				if (err) {
					done(err);
				} else {
					ids = ids.concat(findIds.filter((e, i, self) => {
						return ids.indexOf(e) === -1 && self.indexOf(e) === i;
					}));
					if (ids.length >= MAX) {
						submit(push, done);
					} else {
						done();
					}
				}
			});
		}
	}, (done, push) => {
		if (ids.length) {
			submit(push, err => {
				done(err);
			});
		} else {
			done();
		}
	});

	function buildEntities(ids, push, callback) {
		let r = domainObj(ids, builder.createLoader);
		if (typeof r.get == "function") {
			r = r.get();
		}
		let obj = Object.assign({
			id: "id",
			sql: "select * from dual limit 1",
			joins: {}
		}, r);

		let tasks = [];
		let domains = {};
		console.log("Processing " + ids.length);
		ids.forEach(id => {
			domains[id] = {};
			Object.keys(obj.joins).forEach(name => {
				let t = obj.joins[name];
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
			if (!obj.id) {
				logger.log('[FATAL ERROR]: No ID specified');
			}

			sqlClient.query(obj.sql, (err, results, fields) => {
				if (err) return done(err);
				mapResults(results, fields, row => {
					if (obj.transform) {
						row = obj.transform(row);
					}
					let id = row[obj.id];
					if (!id) {
						logger.error('ID: "' + obj.id + '" not found in object:');
					} else if (!domains[row[obj.id]]) {
						logger.error('ID: "' + obj.id + '" with a value of: "' + row[obj.id] + '" does not match any ID in the domain object. This could be caused by using a WHERE clause on an ID that differs from the SELECT ID');
					} else {
						//We need to keep the domain relationships in tact
						domains[row[obj.id]] = Object.assign(domains[row[obj.id]], row);
					}
				});
				done();
			}, {
				inRowMode: true
			});
		});


		Object.keys(obj.joins).forEach(name => {
			let t = obj.joins[name];
			if (t.type === "one_to_many") {
				tasks.push(done => {
					sqlClient.query(t.sql, (err, results, fields) => {
						if (err) {
							return done(err);
						}
						mapResults(results, fields, row => {
							if (t.transform) {
								row = t.transform(row);
							}
							domains[row[t.on]][name].push(row);
						});
						done();
					}, {
						inRowMode: true
					});
				});
			} else {
				tasks.push(done => {
					sqlClient.query(t.sql, (err, results, fields) => {
						if (err) {
							return done(err);
						}
						mapResults(results, fields, row => {
							if (t.transform) {
								row = t.transform(row);
							}
							domains[row[t.on]][name] = row;
						});
						done();
					}, {
						inRowMode: true
					});
				});
			}
		});
		async.parallelLimit(tasks, 5, (err) => {
			if (err) {
				callback(err);
			} else {
				let needsDrained = false;
				let getEid = opts.getEid || ((id, obj, stats) => stats.end);
				ids.forEach((id, i) => {
					// skip the domain if there is no data with it
					if (Object.keys(domains[id]).length === 0) {
						logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
						return;
					}
					let eid = getEid(id, domains[id], eids);
					let event = {
						event: opts.queue,
						id: opts.id,
						eid: eid,
						payload: domains[id],
						correlation_id: {
							source: opts.source,
							start: opts.isSnapshot ? id : eid,
							end: opts.isSnapshot ? ids[i + 1] : undefined,
							units: 1
						}
					};
					push(event);
				});
				callback();
			}
		});
	}
};

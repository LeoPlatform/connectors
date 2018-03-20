"use strict";
const logger = require("leo-sdk/lib/logger")("leo.connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const leo = require("leo-sdk");
const ls = leo.streams;

const builder = require("./loaderBuilder.js");

module.exports = function(sqlClient, sql, domainObj, opts = {
	source: "loader",
	isSnapshot: false
}) {
	let pass = new PassThrough({
		objectMode: true
	});
	const MAX = 5000;
	let ids = [];

	function submit(done) {
		async.doWhilst((done) => {
			let buildIds = ids.splice(0, MAX);
			buildEntities(buildIds, done);
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
	return ls.pipeline(ls.through((obj, done) => {
		eids.start = eids.start || obj.eid;
		eids.end = obj.eid;
		let tasks = [];
		let findIds = [];

		Object.keys(sql).forEach(key => {
			if (sql[key] === true && key in obj.payload) {
				findIds = findIds.concat(obj.payload[key]);
			} else if (key in obj.payload) {
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
					submit(done);
				} else {
					done();
				}
			}
		});
	}, (done) => {
		if (ids.length) {
			submit(err => {
				if (err) {
					pass.end(err);
					done(err);
				} else {
					pass.end();
					done();
				}
			});
		} else {
			pass.end();
			done();
		}
	}), pass);

	function buildEntities(ids, callback) {
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

		tasks.push(done => {
			sqlClient.query(obj.sql, (err, results) => {
				if (!err) {
					let row;
					for (let i in results) {
						if (obj.transform) {
							row = obj.transform(results[i]);
						} else {
							row = results[i];
						}

						try {
							Object.assign(domains[row[obj.id]], row);
						} catch (err) {
							if (!obj.id) {
								logger.log('[FATAL ERROR]: No ID specified');
							} else if (!row[obj.id]) {
								logger.log('[FATAL ERROR]: ID: "' + obj.id + '" not found in object:');
								logger.log(row);
							} else if (!domains[row[obj.id]]) {
								logger.log('[FATAL ERROR]: ID: "' + obj.id + '" with a value of: "' + row[obj.id] + '" does not match any ID in the domain object. This could be caused by using a WHERE clause on an ID that differs from the SELECT ID');
							}

							throw new Error(err);
						}
					}
				}

				done(err);
			});
		});


		Object.keys(obj.joins).forEach(name => {
			let t = obj.joins[name];
			if (t.type === "one_to_many") {
				tasks.push(done => {
					sqlClient.query(t.sql, (err, results) => {
						if (err) {
							return done(err);
						}
						for (let i = 0; i < results.length; i++) {
							let row;
							if (t.transform) {
								row = t.transform(results[i]);
							} else {
								row = results[i];
							}
							domains[row[t.on]][name].push(row);
						}
						done();
					});
				});
			} else {
				tasks.push(done => {
					sqlClient.query(t.sql, (err, results) => {
						if (err) {
							return done(err);
						}
						for (let i = 0; i < results.length; i++) {
							let row;
							if (t.transform) {
								row = t.transform(results[i]);
							} else {
								row = results[i];
							}
							//console.log(t.on, name, row)
							domains[row[t.on]][name] = row;
						}
						done();
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

					if (!pass.write(event)) {
						needsDrained = true;
					}
				});

				if (needsDrained) {
					pass.once('drain', callback);
				} else {
					callback();
				}
			}
		});
	}
};

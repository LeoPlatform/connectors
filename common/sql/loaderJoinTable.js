"use strict";
const logger = require("leo-sdk/lib/logger")("leo.connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const leo = require("leo-sdk");
const ls = leo.streams;

const builder = require("./loaderBuilder.js");
module.exports = function(sqlClient, idKeys, domainObj, opts = {
	source: "loader",
	isSnapshot: false
}) {
	return ls.through({
		highWaterMark: opts.isSnapshot ? 2 : 16
	}, (obj, done, push) => {
		buildEntities(obj, push, done);
	});

	function buildEntities(cmd, push, callback) {
		let r = domainObj(cmd.jointable, builder.createLoader);
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
		console.log("Processing " + cmd.count);

		let template = {};
		Object.keys(obj.joins).forEach(name => {
			let t = obj.joins[name];
			if (t.type === "one_to_many") {
				template[name] = [];
			} else {
				template[name] = {};
			}
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
					} else {
						//We need to keep the domain relationships in tact
						domains[row[obj.id]] = Object.assign({}, template, row);
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
				let ids = Object.keys(domains);

				function getId(o) {
					if (Array.isArray(idKeys)) {
						return {
							[idKeys[0]]: o[idKeys[0]],
							[idKeys[1]]: o[idKeys[1]]
						};
					} else {
						return o[idKeys];
					}
				}

				ids.forEach((id, i) => {
					// skip the domain if there is no data with it
					if (Object.keys(domains[id]).length === 0) {
						logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
						return;
					}
					let current = domains[id];
					let next = domains[[ids[i + 1]]];

					let event = {
						event: opts.queue,
						id: opts.id,
						eid: cmd.eid,
						payload: current,
						correlation_id: {
							source: opts.source,
							start: opts.isSnapshot ? getId(current) : cmd.eid,
							end: opts.isSnapshot && next ? getId(next) : cmd.eid,
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

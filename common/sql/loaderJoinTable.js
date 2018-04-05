"use strict";
const logger = require("leo-sdk/lib/logger")("leo.connector.sql");
const async = require("async");
const leo = require("leo-sdk");
const merge = require("lodash/merge");
const lib = require("./loader/lib");
const ls = leo.streams;

const MAX = 5000;

const builder = require("./loaderBuilder.js");

const rowValue = (row, id) => {
	const value = row[id];
	const err = !value;
	if (err) {
		logger.error('ID: "' + id + '" not found in object:');
	}
	return {
		value,
		err
	};
};

const domainIdentifierFor = (row, key) => {
	let domainIdentifier;
	if (Array.isArray(key)) {
		const {
			value: primary,
			err: primaryErr
		} = rowValue(row, key[0]);
		const {
			value: secondary,
			err: secondaryErr
		} = rowValue(row, key[1]);
		if (primaryErr || secondaryErr) return {
			err: true
		};
		domainIdentifier = `${primary}-${secondary}`;
	} else {
		const {
			value,
			err
		} = rowValue(row, key);
		if (err) return {
			err
		};
		domainIdentifier = value;
	}
	return {
		domainIdentifier
	};
};


module.exports = function(sqlClient, idKeys, sql, domainObj, opts = {
	source: "loader",
	isSnapshot: false
}) {
	let ids = [];

	function submit(push, done) {
		async.doWhilst((done) => {
			let buildIds = ids.splice(0, MAX);
			buildEntities({
				jointable: 'values ' + buildIds.map(keys => {
					return "(" + keys.join(",") + ")";
				}).join(","),
				count: buildIds.length
			}, push, done);
		}, () => ids.length >= MAX, (err) => {
			if (err) {
				done(err);
			} else {
				done();
			}
		});
	}


	return ls.through({
		highWaterMark: opts.isSnapshot ? 2 : 16
	}, (obj, done, push) => {
		if (obj.jointable) {
			buildEntities(obj, push, done);
		} else {
			lib.processIds(sqlClient, obj, sql, idKeys, (err, newIds) => {
				if (err) {
					console.log(err);
				}
				ids = ids.concat(newIds);
				if (ids.length >= MAX) {
					submit(push, done);
				} else {
					done();
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
					const {
						domainIdentifier,
						err
					} = domainIdentifierFor(row, obj.id);
					if (!err) {
						//We need to keep the domain relationships in tact
						domains[domainIdentifier] = merge({}, template, domains[domainIdentifier], row);
					}
				});
				done();
			}, {
				inRowMode: true
			});
		});

		const mapResultsRow = (t, name) => {
			return row => {
				if (t.transform) {
					row = t.transform(row);
				}
				const {
					domainIdentifier,
					err
				} = domainIdentifierFor(row, t.on);
				if (typeof domains[domainIdentifier] === 'undefined') {
					domains[domainIdentifier] = merge({}, template);
				}
				if (!err) {
					if (t.type === "one_to_many") {
						domains[domainIdentifier][name].push(row);
					} else {
						domains[domainIdentifier][name] = row;
					}
				}
			};
		};

		Object.keys(obj.joins).forEach(name => {
			let t = obj.joins[name];
			tasks.push(done => {
				sqlClient.query(t.sql, (err, results, fields) => {
					if (err) {
						return done(err);
					}
					mapResults(results, fields, mapResultsRow(t, name));
					done();
				}, {
					inRowMode: true
				});
			});

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

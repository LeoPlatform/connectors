"use strict";
const logger = require("leo-sdk/lib/logger")("connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const leo = require("leo-sdk");
const ls = leo.streams;

module.exports = function(connect, sql, domainObj) {

	let log = logger.sub("process");

	let sqlClient = connect();

	let pass = new PassThrough({
		objectMode: true
	});
	let count = 0;
	const MAX = 5000;
	let ids = [];


	function submit(done) {
		async.doWhilst((done) => {
			let buildIds = ids.splice(0, MAX);
			buildEntities(buildIds, done);
		}, () => ids.length >= MAX, (err) => {
			if (err) {
				console.log(err);
				done(err);
			} else {
				done();
			}
		});
	}

	return ls.pipeline(ls.through((obj, done) => {
		let tasks = [];
		let findIds = [];
		Object.keys(sql).forEach(key => {
			if (sql[key] === true && key in obj) {
				findIds = findIds.concat(obj[key]);
			} else if (key in obj) {
				tasks.push((done) => {
					log.debug(obj, key);
					let s = sql[key].replace(/__IDS__/, obj[key].join());
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
				console.log(err);
				done(err);
			} else {
				ids = ids.concat(findIds.filter((e) => {
					return ids.indexOf(e) === -1;
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
				pass.end();
				done(err);
			});
		} else {
			pass.end();
			done();
		}
	}), pass);


	function buildEntities(ids, callback) {
		let obj = Object.assign({
			id: "id",
			sql: "select * from dual limit 1",
			hasMany: {},
			hasOne: {}
		}, domainObj(ids));


		let tasks = [];
		let domains = {};
		ids.forEach(id => {
			domains[id] = {};
			Object.keys(obj.hasMany).forEach(name => {
				domains[id][name] = [];
			});
			for (var name in obj.hasOne) {
				domains[id][name] = {};
			}
		});

		tasks.push(done => {
			sqlClient.query(obj.sql, (err, results) => {
				for (var i = 0; i < results.length; i++) {
					Object.assign(domains[results[i][obj.id]], results[i]);
				}
				done();
			});
		});

		Object.keys(obj.hasMany).forEach(name => {
			tasks.push(done => {
				let t = obj.hasMany[name];
				sqlClient.query(t.sql, (err, results) => {
					if (err) {
						return done(err);
					}
					for (var i = 0; i < results.length; i++) {
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
		});

		Object.keys(obj.hasOne).forEach(name => {
			tasks.push(done => {
				let t = obj.hasOne[name];
				sqlClient.query(t.sql, (err, results) => {
					if (err) {
						return done(err);
					}
					for (var i = 0; i < results.length; i++) {
						let row;
						if (t.transform) {
							row = t.transform(results[i]);
						} else {
							row = results[i];
						}

						domains[row[t.on]][name] = row;
					}
					done();
				});
			});
		});
		async.parallelLimit(tasks, 5, (err) => {
			let needsDrained = false;
			for (var id in domains) {
				if (!pass.write(domains[id])) {
					needsDrained = true;
				}
			}


			if (needsDrained) {
				pass.once('drain', callback);
			} else {
				callback();
			}
		});
	}


};
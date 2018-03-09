"use strict";

const leo = require("leo-sdk");
const ls = leo.streams;
const combine = require("./combine.js");
const async = require("async");
module.exports = function(client, tableConfig, stream, callback) {
	ls.pipe(stream, combine(), ls.through((obj, done) => {
		let tasks = [];
		Object.keys(obj).forEach(t => {
			console.log("TABLE IS ", t);
			if (t in tableConfig) {
				tasks.push(done => {
					let config = tableConfig[t];
					let sk = null;
					let nk = [];
					let scds = {
						0: [],
						1: [],
						2: [],
						6: []
					};
					Object.keys(config.structure).forEach(f => {
						let field = config.structure[f];

						if (field == "sk" || field.sk) {
							sk = f;
						} else if (field.nk) {
							nk.push(f);
						} else if (field.scd !== undefined) {
							scds[field.scd].push(f);
						}
					});
					if (tableConfig[t].isDimension) {
						client.importDimension(obj[t].stream, t, sk, nk, scds, done);
					} else {
						client.importFact(obj[t].stream, t, nk, done);
					}
				});
			}
		});

		async.parallelLimit(tasks, 10, (err) => {
			if (err) {
				done(err);
			} else {
				let tasks = [];
				Object.keys(obj).forEach(t => {
					let config = tableConfig[t];
					let links = {};
					Object.keys(config.structure).forEach(f => {
						let field = config.structure[f];
						if (field.dimension) {
							links[f] = field.dimension;
						}
					});
					if (Object.keys(links).length) {
						tasks.push(done => client.linkDimensions(t, links, done));
					}
				});

				async.parallelLimit(tasks, 10, (err) => {
					console.log("HERE------------------------------");

					done(err);
				});
			}
		});
	}), err => {
		console.log("IN HERE!!!!!!!");
		callback(err, "ALL DONE");
	});
};

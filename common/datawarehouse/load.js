"use strict";

const leo = require("leo-sdk");
const ls = leo.streams;
const combine = require("./combine.js");
const async = require("async");

module.exports = function(client, tableConfig, stream, callback) {
	let tableStatuses = {};
	let tableSks = {};
	let tableNks = {};
	Object.keys(tableConfig).forEach(t => {
		let config = tableConfig[t];
		//console.log("config", t, JSON.stringify(config, null, 2));
		Object.keys(config.structure).forEach(f => {
			let field = config.structure[f];
			if (field == "sk" || field.sk) {
				tableSks[t] = f;
			} else if (field.nk) {
				if (!(t in tableNks)) {
					tableNks[t] = [];
				}
				tableNks[t].push(f);
			}
		});
	});

	let checkforDelete = ls.through(function(obj, done) {
		if (obj.payload.type == "delete") {
			let data = obj.payload.data || {};
			let ids = data.in || [];
			let entities = data.entities || [];
			ids.map(id => {
				entities.map(entity => {
					let field = entity.field || "id";
					this.push(Object.assign({}, obj, {
						payload: {
							type: entity.type,
							entity: entity.name,
							command: "delete",
							field: field,
							data: {
								id: field == "id" ? id : `_del_${id}`,
								__leo_delete__: field,
								__leo_delete_id__: id
							}
						}
					}));
				});
			});
			done();
		} else {
			done(null, obj);
		}
	});

	let usedTables = {};
	ls.pipe(stream, checkforDelete, combine(tableNks), ls.write((obj, done) => {
		let tasks = [];
		Object.keys(obj).forEach(t => {
			if (t in tableConfig) {
				usedTables[t] = true;
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
						client.importDimension(obj[t].stream, t, sk, nk, scds, (err, tableInfo) => {
							if (!err && tableInfo && tableInfo.count === 0) {
								tableStatuses[t] = "First Load";
							}
							done(err);
						}, tableConfig[t]);
					} else {
						client.importFact(obj[t].stream, t, nk, (err, tableInfo) => {
							if (!err && tableInfo && tableInfo.count === 0) {
								tableStatuses[t] = "First Load";
							}
							done(err);
						}, tableConfig[t]);
					}
				});
			}
		});

		async.parallelLimit(tasks, 10, (err) => {
			if (err) {
				done(err);
			} else {
				client.insertMissingDimensions(usedTables, tableConfig, tableSks, tableNks, (missingDimError) => {
					if (missingDimError) {
						return done(missingDimError);
					}
					let tasks = [];
					Object.keys(obj).forEach(t => {
						let config = tableConfig[t];
						let sk = null;
						let nk = [];
						let scds = {
							0: [],
							1: [],
							2: [],
							6: []
						};
						let links = [];
						//if (!config || !config.structure) console.log(t);
						config && config.structure && Object.keys(config.structure).forEach(f => {
							let field = config.structure[f];

							if (field == "sk" || field.sk) {
								sk = f;
							} else if (field.nk) {
								nk.push(f);
							} else if (field.scd !== undefined) {
								scds[field.scd].push(f);
							}
							if (field.dimension) {
								let link = {};
								if (typeof field.dimension == "string") {
									link = {
										table: field.dimension,
										source: f
									};
									let nks = tableNks[field.dimension];
									if (nks && nks.length == 1) {
										link.on = nks[0];
									}
								}
								links.push(Object.assign({
									table: null,
									on: f,
									destination: client.getDimensionColumn(f, field),
									link_date: "_auditdate",
									sk: tableSks[link.table]
								}, link));
							}
						});
						if (links.length) {
							tasks.push(done => client.linkDimensions(t, links, nk, done, tableStatuses[t] || "Unmodified"));
						}
					});
					async.parallelLimit(tasks, 10, (err) => {
						console.log("HERE------------------------------", err);
						done(err);
					});
				});
			}
		});
	}), err => {
		console.log("IN HERE!!!!!!!");
		callback(err, "ALL DONE");
	});
};

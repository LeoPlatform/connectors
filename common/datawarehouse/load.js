"use strict";

const logger = require('leo-logger');
const leo = require("leo-sdk");
const ls = leo.streams;
const streams = require('leo-streams');
const combine = require("./combine.js");
const async = require("async");
const validate = require('./../utils/validation');
let errorStream;

module.exports = function (ID, source, client, tableConfig, stream, callback) {

	// adding backwards compatibility for ID and source
	if (!callback) {
		callback = tableConfig;
		stream = client;
		tableConfig = source;
		client = ID;
		source = 'queue:dw.load';
		ID = 'system:dw-ingest';
	}

	let tableStatuses = {};
	let tableSks = {};
	let tableNks = {};
	Object.keys(tableConfig).forEach(t => {
		let config = tableConfig[t];
		Object.keys(config.structure).forEach(f => {
			let field = config.structure[f];
			if (field === "sk" || field.sk) {
				tableSks[t] = f;
			} else if (field.nk) {
				if (!(t in tableNks)) {
					tableNks[t] = [];
				}
				tableNks[t].push(f);
			}
		});
	});

	let checkforDelete = ls.through(function (obj, done) {
		if (obj.payload.type === "delete") {
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
								id: field === 'id' ? id : `_del_${id}`,
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

	let validateData = ls.through(function (obj, done) {
		let eventObj = obj.payload.data;
		let invalid = false;

		if (obj.payload.type === 'delete') {
			if (eventObj.in && Array.isArray(eventObj.in) && eventObj.in.length) {
				invalid = eventObj.in.some(id => {
					if (typeof id !== 'number' && typeof id !== 'string') {
						// invalid event
						return true;
					}
				});
			} else {
				invalid = handleFailedValidation(ID, source, obj, 'No events id’s in delete.');
			}
		} else if (!obj || !obj.payload || !obj.payload.table && !obj.payload.entity) {
			invalid = handleFailedValidation(ID, source, obj, 'Invalid payload.');
		} else {
			let tableName = obj.payload.table || obj.payload.entity;
			let table;

			// find the table name
			// first check the config object key
			if (tableConfig[tableName]) {
				table = tableConfig[t];
			} else {
				// then check the config label
				Object.keys(tableConfig).some(entity => {
					if (tableConfig[entity].label === tableName) {
						if ((tableConfig[entity].isDimension && obj.payload.type === 'dimension') || (obj.payload.type === 'fact' && !tableConfig[entity].isDimension)) {
							table = tableConfig[entity];
							return true;
						}
					}
				});
			}

			// note: this returns TRUE when something is invalid.
			invalid = Object.keys(eventObj).some(field => {
				// if we cannot find a matching table, it’s an invalid record.
				if (!table) {
					return handleFailedValidation(ID, source, obj, `No table found for ${tableName}`);
				}

				let type = table.structure[field] && table.structure[field].type.match(/(\w+)(\((\d+)\))?/) || [undefined, undefined];
				let fieldDefault = table.structure[field] && table.structure[field].default || null;
				let value = eventObj[field];

				if (value !== null) {
					switch (type[1]) {
						case 'varchar':
							if (!validate.isValidString(value, type[3] && type[3] || 255, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid String on field ${field}`);
							}

							// check for enum and validate if exists
							if (table.structure[field].sort && table.structure[field].sort.values && !validate.isValidEnum(value, table.structure[field].sort.values, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid enum on field ${field}`);
							}
							break;

						case 'timestamp':
							if (!validate.isValidTimestamp(value, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} on field ${field}`);
							}
							break;

						case 'datetime':
							if (!validate.isValidDatetime(value, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} on field ${field}`);
							}
							break;

						case 'integer':
							if (!validate.isValidInteger(value, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} on field ${field}`);
							}
							break;

						case 'bigint':
							if (!validate.isValidBigint(value, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} on field ${field}`);
							}
							break;

						case 'float':
							if (!validate.isValidFloat(value, fieldDefault)) {
								return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} on field ${field}`);
							}
							break;

						case undefined:
							return handleFailedValidation(ID, source, obj, `Invalid ${type[1]} in the table config for table: ${table.identifier} field ${field}`);
					}
				}
			});
		}

		if (invalid) {
			logger.error('Record has error');
			done();
		} else {
			done(null, obj);
		}
	});

	let usedTables = {};
	ls.pipe(stream, validateData, checkforDelete, combine(tableNks), ls.write((obj, done) => {
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

						if (field === "sk" || field.sk) {
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
						config && config.structure && Object.keys(config.structure).forEach(f => {
							let field = config.structure[f];

							if (field === "sk" || field.sk) {
								sk = f;
							} else if (field.nk) {
								nk.push(f);
							} else if (field.scd !== undefined) {
								scds[field.scd].push(f);
							}
							if (field.dimension) {
								let link = {};
								if (typeof field.dimension === 'string') {
									link = {
										table: field.dimension,
										source: f
									};
									let nks = tableNks[field.dimension];
									if (field.on && typeof field.on === 'object' && !Array.isArray(field.on)) {
										link.on = [];
										link.source = [];
										Object.entries(field.on).map(([key, val]) => {
											link.on.push(val);
											link.source.push(key);
										})
									} else if (nks && nks.length === 1) {
										link.on = nks[0];
									} else if (nks && nks.length > 1) {
										link.on = nks;
										link.source = field.on;
									}
								}
								links.push(Object.assign({
									table: null,
									join_id: f,
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
		// close the error stream if open
		if (errorStream) {
			errorStream.end(streamError => {
				if (streamError) {
					logger.error('Closing error stream with error', streamError);
				} else {
					logger.log('Closing error stream');
				}
			});
		}

		callback(err, "ALL DONE Ingesting");
	});
};

/**
 * Pass the failed object onto a failed queue.
 * @param ID {string}
 * @param source {string}
 * @param eventObj {Object}
 * @param error {string}
 */
function handleFailedValidation(ID, source, eventObj, error) {
	if (!errorStream) {
		errorStream = streams.passthrough({
			objectMode: true
		});

		streams.pipe(
			errorStream,

			ls.process(ID, obj => {
				return obj;
			}),

			leo.load(ID, `${source}_error`),

			(err) => {
				err && logger.err('GOT ERROR', err);
			}
		);
	}

	logger.debug('Adding failed event', eventObj);
	// write the error to the payload so it gets passed on
	eventObj.payload.error = error;
	errorStream.write(eventObj);

	return true;
}

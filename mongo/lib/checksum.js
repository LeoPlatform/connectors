"use strict";
const crypto = require('crypto');
var base = require("leo-connector-common/checksum/lib/handler.js");
var moment = require("moment");
require("moment-timezone");

var mongodb = require("mongodb");
var MongoClient = mongodb.MongoClient;
var ObjectID = mongodb.ObjectID;

module.exports = function () {
	return base({
		batch: wrap(batch),
		individual: wrap(individual),
		sample: wrap(sample),
		nibble: wrap(nibble),
		range: wrap(range),
		initialize: wrap(initialize),
		destroy: wrap(destroy),
	});

	function wrap(method) {
		return function (event, callback) {
			method(event, (err, data) => {
				if (cacheDB) {
					console.log("Closing Database");
					cacheDB.close();
					cacheDB = null;
				}
				callback(err, data);
			});
		}
	};

	function batch(event, callback) {
		console.log("Calling Batch", event);

		var startTime = moment.now();
		var data = event.data;
		var settings = event.settings;
		var id = (value) => {
			return settings.id_column === "_id" ? new ObjectID(value) : value
		};
		getCollection(settings).then(collection => {

			var extract = (obj) => {
				return settings.fields.map(f => obj[f]);
			};

			if (typeof settings.fields === "string") {
				extract = eval(`(${settings.fields})`);
			}
			var result = {
				qty: 0,
				ids: data.ids,
				start: data.start,
				end: data.end,
				hash: [0, 0, 0, 0]
			};

			var where = {};
			if (data.start || data.end) {
				where[settings.id_column] = {};

				if (data.start) {
					where[settings.id_column].$gte = id(data.start);
				}
				if (data.end) {
					where[settings.id_column].$lte = id(data.end);
				}
			}
			var cursor = collection.find(where, settings.projectionFields || {}, {
				'sort': [
					[settings.id_column, 1]
				]
			}).stream();

			cursor.on("end", () => {
				result.duration = moment.now() - startTime;
				callback(null, result);
			}).on("error", (err) => {
				console.log("Batch On Error", err)
				callback(err);
			}).on("data", (obj) => {
				var allFields = "";
				extract(obj).forEach(value => {
					if (value instanceof Date) {
						allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
					} else if (value !== null && value !== undefined && value.toString) {
						allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
					} else {
						allFields += " ";
					}
				});

				var hash = crypto.createHash('md5').update(allFields).digest();

				result.hash[0] += hash.readUInt32BE(0);
				result.hash[1] += hash.readUInt32BE(4);
				result.hash[2] += hash.readUInt32BE(8);
				result.hash[3] += hash.readUInt32BE(12);
				result.qty += 1;
			});
		}).catch(callback);
	}

	function individual(event, callback) {
		console.log("Calling Individual", event);
		var startTime = moment.now();
		var data = event.data;
		var settings = event.settings;
		var id = (value) => {
			return settings.id_column === "_id" ? new ObjectID(value) : value
		};
		getCollection(settings).then(collection => {

			var extract = (obj) => {
				return settings.fields.map(f => obj[f]);
			};

			if (typeof settings.fields === "string") {
				extract = eval(`(${settings.fields})`);
			}

			var where = {
				[settings.id_column]: {
					$gte: id(data.start),
					$lte: id(data.end)
				}
			};
			var results = {
				ids: data.ids,
				start: data.start,
				end: data.end,
				qty: 0,
				checksums: []
			};

			var idFn = typeof settings.idFunction === "string" ? eval(`(${settings.idFunction})`) : (obj) => obj[settings.id_column];

			var cursor = collection.find(where, settings.projectionFields || {}, {
				'sort': [
					[settings.id_column, 1]
				]
			}).stream();

			cursor.on("end", () => {
				results.duration = moment.now() - startTime;
				callback(null, results);
			}).on("error", (err) => {
				console.log("Individual On Error", err)
				callback(err);
			}).on("data", (obj) => {
				var allFields = "";

				extract(obj).forEach(value => {
					if (value instanceof Date) {
						allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
					} else if (value !== null && value !== undefined && value.toString) {
						allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
					} else {
						allFields += " ";
					}
				});
				var hash = crypto.createHash('md5').update(allFields).digest('hex');
				results.checksums.push({
					id: idFn(obj), //[settings.id_column],
					_id: settings._id_column ? obj[settings._id_column] : undefined,
					hash: hash
				});
				results.qty += 1;
			});

		}).catch(callback);
	}

	function sample(event, callback) {
		console.log("Calling Sample", event);
		var data = event.data;
		var settings = event.settings;

		getCollection(settings).then(collection => {

			var extract = (obj) => {
				return settings.fields.map(f => obj[f]);
			};
			if (typeof settings.fields === "string") {
				extract = eval(`(${settings.fields})`);
			}

			var results = {
				qty: 0,
				ids: [],
				start: data.start,
				end: data.end,
				checksums: []
			};

			var where = {};

			if (data.ids) {
				if (settings.id_column === "_id") {
					where = {
						[settings.id_column]: {
							$in: data.ids.map(f => new ObjectID(f))
						}
					};
				} else {
					where = {
						[settings.id_column]: {
							$in: data.ids
						}
					};
				}
			} else {
				where = {
					[settings.id_column]: {
						$gte: data.start,
						$lte: data.end
					}
				};
			}
			var idFn = settings.idFunction === "string" ? eval(`(${settings.idFunction})`) : (obj) => obj[settings.id_column];
			var cursor = collection.find(where, settings.projectionFields || {}, {
				'sort': [
					[settings.id_column, 1]
				]
			}).stream();

			cursor.on("end", function () {
				callback(null, results);
			}).on("err", function (err) {
				console.log("error");
				throw err;
			}).on("data", function (obj) {
				var out = [];
				extract(obj, "sample").forEach(value => {
					if (value instanceof Date) {
						out.push(Math.round(value.getTime() / 1000) + "  " + moment(value).utc().format());
					} else if (value && typeof value === "object" && value.toHexString) {
						out.push(value.toString());
					} else {
						out.push(value);
					}
				});

				results.ids.push(idFn(obj));
				results.checksums.push(out);
				results.qty += 1;
			});

		}).catch(callback);
	}

	function range(event, callback) {
		console.log("Calling Range", event);

		//var [data, settings, collection] = [event.data, event.settings, getCollection(settings)]
		var data = event.data;
		var settings = event.settings;
		var id = (value) => {
			return settings.id_column === "_id" ? new ObjectID(value) : value
		};
		getCollection(settings).then(collection => {
			var max = {};
			var min = {};
			var total = {};

			if (data.start || data.end) {
				total[settings.id_column] = {};
			}
			if (data.start) {
				min[settings.id_column] = {
					$gte: id(data.start)
				};
				total[settings.id_column].$gte = id(data.start);
			}
			if (data.end) {
				max[settings.id_column] = {
					$lte: id(data.end)
				};
				total[settings.id_column].$lte = id(data.end);
			}
			collection.findOne(min, {
				[settings.id_column]: 1
			}, {
				'sort': [
					[settings.id_column, 1]
				],
				"limit": 1
			}, (err, start) => {
				if (err) {
					return callback(err);
				}
				collection.findOne(max, {
					[settings.id_column]: 1
				}, {
					'sort': [
						[settings.id_column, -1]
					],
					"limit": 1
				}, (err, end) => {
					if (err) {
						return callback(err);
					}
					collection.count(total, (err, total) => {
						if (err) {
							return callback(err);
						}
						callback(null, {
							min: start[settings.id_column] || 0,
							max: end[settings.id_column],
							total: total
						});
					});
				});
			});
		}).catch(callback);
	}

	function nibble(event, callback) {
		console.log("Calling Nibble", event);

		var data = event.data;
		var settings = event.settings;
		var id = (value) => {
			return (settings.id_column === "_id" && value != null && value != undefined) ? new ObjectID(value) : value;
		};

		getCollection(settings).then(collection => {
			var where = {
				[settings.id_column]: {
					$gte: id(data.start),
					$lte: id(data.end)
				}
			};
			collection.find(where, {
				[settings.id_column]: 1
			}, {
				sort: [
					[settings.id_column, !data.reverse ? 1 : -1]
				],
				limit: 2,
				skip: data.limit - 1
			}).toArray((err, rows) => {
				if (err) {
					return callback(err);
				}
				data.current = rows[0] ? rows[0][settings.id_column] : null;
				data.next = rows[1] ? rows[1][settings.id_column] : null;
				callback(null, data)
			});
		}).catch(callback);
	}

	function initialize(event, callback) {
		console.log("Calling Initialize", event)
		callback(null, {});
	}

	function destroy(event, callback) {
		console.log("Calling Destroy", event);
		callback()
	}

	var cacheDB;

	function getCollection(settings) {
		var opts = Object.assign({}, settings);
		console.log("Connection Info", opts.database, opts.collection)
		return new Promise((resolve, reject) => {
			MongoClient.connect(opts.database, (err, db) => {
				if (err) {
					reject(err);
				} else {
					cacheDB = db;
					resolve(db.collection(opts.collection));
				}
			})
		});
	}

	function escape(value) {
		if (typeof value === "string") {
			return "'" + value + "'";
		}
		return value;
	}

	function getFields(connection, event) {
		console.log(event);
		var settings = event.settings;
		var tableName = getTable(event);
		return new Promise((resolve, reject) => {
			var typesQuery = `SELECT column_name as field, DATA_TYPE as type, CHARACTER_MAXIMUM_LENGTH length FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${tableName}';`
			connection.query(typesQuery, (err, types) => {
				if (err) {
					console.log("Batch Get Types Error", err);
					reject(err);
				} else {
					var fields = [];
					var table = types.reduce((obj, row) => {
						obj[row.field] = row;
						return obj;
					}, {});
					var fieldCalcs = settings.fields.map(field => {
						var def = table[field];
						if (!def) {
							return "' '";
						}
						fields.push(def.field);
						switch (def.type.toLowerCase()) {
							case "date":
							case "datetime":
							case "timestamp":
								return `coalesce(md5(floor(UNIX_TIMESTAMP(${def.field}))))`;
							default:
								return `coalesce(md5(${def.field}), " ")`;
								break;
						}
					});
					resolve({
						fieldCalcs: fieldCalcs,
						fields: fields
					});
				}
			});
		});
	}

	function where(data, settings) {
		var where = "";
		if (data.ids) {
			where = `where ${settings.id_column} in (${data.ids.map(f => escape(f))})`
		} else if (data.start || data.end) {
			var parts = [];
			if (data.start) {
				parts.push(`${settings.id_column} >= ${escape(data.start)}`);
			}
			if (data.end) {
				parts.push(`${settings.id_column} <= ${escape(data.end)}`);
			}
			where = "where " + parts.join(" and ");
		}

		if (settings.where) {
			if (where.trim() != '') {
				where += " and ";
			} else {
				where = "where ";
			}
			where += buildWhere(settings.where);
		}
		return where;
	}

	function buildWhere(where, combine) {
		combine = combine || "and";
		if (where) {
			var w = [];
			if (typeof where == "object" && where.length) {
				where.forEach(function (e) {
					if (typeof e != "object") {
						w.push(e);
					} else if ("_" in e) {
						w.push(e._);
					} else if ("or" in e) {
						w.push(buildWhere(e.or, "or"));
					} else if ("and" in e) {
						w.push(buildWhere(e.and));
					} else {
						w.push(`${e.field} ${e.op || "="} ${escape(e.value)}`);
					}
				});
			} else if (typeof where == "object") {
				for (var k in where) {
					var entry = where[k];
					var val = "";
					var op = "="

					if (typeof(entry) !== "object") {
						val = entry;
					} else if ("or" in entry) {
						w.push(buildWhere(entry.or, "or"));
					} else if ("and" in entry) {
						w.push(buildWhere(entry.and));
					} else {
						k = entry.field || k;
						val = entry.value;
						op = entry.op || op;
					}

					w.push(`${k} ${op} ${escape(val)}`);
				}
			} else {
				w.push(where);
			}

			var joined = w.join(` ${combine} `);
			return `(${joined})`;
		}
		return ""
	}
};
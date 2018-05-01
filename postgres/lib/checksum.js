"use strict";
const uuid = require('uuid');
const base = require("leo-connector-common/checksum/lib/handler.js");
const moment = require("moment");

let fieldTypes = {
	INT4: 23,
	VARCHAR: 1043,
	DATE: 1082,
	TIME: 1083,
	TIMESTAMP: 1114,
};
let fieldIds = {};
Object.keys(fieldTypes).forEach(key => {
	fieldIds[fieldTypes[key]] = key;
});

module.exports = function(connection, fieldsTable) {
	if (!connection.end && connection.disconnect) {
		connection.end = connection.disconnect;
	}

	return base({
		batch: batch,
		individual: individual,
		sample: sample,
		nibble: nibble,
		range: range,
		initialize: initialize,
		destroy: destroy,
		delete: del
	});

	function getConnection() {
		return connection;
	}

	function batch(event, callback) {
		let startTime = moment.now();

		let data = event.data;
		let settings = event.settings;
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let fieldCalcs = table.fieldCalcs;
			let batchQuery = `select count(*) as count,
				sum(('x' || substring(hash, 1, 8))::bit(32)::bigint) as sum1,
				sum(('x' || substring(hash, 9, 8))::bit(32)::bigint) as sum2,
				sum(('x' || substring(hash, 17, 8))::bit(32)::bigint) as sum3,
				sum(('x' || substring(hash, 25, 8))::bit(32)::bigint) as sum4
			from (
				select md5(concat(${fieldCalcs.join(', ')})) as hash
				from (${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}) i
			) as t`;

			console.log("Batch Query", batchQuery);
			connection.query(batchQuery, (err, rows) => {
				if (err) {
					console.error("Batch Checksum Error", err);
					callback(err);
				} else {
					callback(null, {
						ids: data.ids,
						start: data.start,
						end: data.end,
						duration: moment.now() - startTime,
						qty: rows[0].count,
						hash: [rows[0].sum1, rows[0].sum2, rows[0].sum3, rows[0].sum4]
					});
				}
			});
		}).catch(callback);
	}

	function individual(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let fieldCalcs = table.fieldCalcs;
			let individualQuery = `select ${settings.id_column} as id, md5(concat(${fieldCalcs.join(', ')})) as hash
				from (${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}) i`;

			console.log("Individual Query", individualQuery);
			connection.query(individualQuery, (err, rows) => {
				if (err) {
					console.error("Individual Checksum Error", err);
					callback(err);
				} else {
					var results = {
						ids: data.ids,
						start: data.start,
						end: data.end,
						qty: rows.length,
						checksums: []
					};
					rows.forEach((row) => {
						results.checksums.push({
							id: row.id,
							hash: row.hash
						});
					});
					callback(null, results);
				}
			});
		}).catch(callback);
	}

	function del(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let delQuery = `delete from ${tableName}
                where ${settings.id_column} in (${data.ids.map(f=>escape(f))})`;

			console.log("Delete Query", delQuery);
			connection.query(delQuery, (err) => {
				if (err) {
					console.error("Delete Error", err);
					callback(err);
					return;
				}
				let results = {
					ids: data.ids
				};
				callback(null, results);
			});
		}).catch(callback);
	}

	function sample(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let sampleQuery = table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings));
			console.log("Sample Query", sampleQuery);
			connection.query(sampleQuery, (err, rows) => {
				if (err) {
					console.log("Sample Error", err);
					callback(err);
					return;
				}
				let results = {
					ids: data.ids,
					start: data.start,
					end: data.end,
					qty: rows.length,
					checksums: rows.map(row => {
						return Object.keys(row).map(f => {
							return row[f]
						});
					})
				};
				callback(null, results);
			});
		}).catch(callback);
	}

	function range(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		let where = [];
		let whereStatement = "";
		if (data.min) {
			where.push(`${settings.id_column} >= ${escape(data.min)}`);
		}
		if (data.max) {
			where.push(`${settings.id_column} <= ${escape(data.max)}`);
		}
		if (where.length) {
			whereStatement = ` where ${where.join(" and ")} `;
		}
		let query = `select MIN(${settings.id_column}) as min, MAX(${settings.id_column}) as max, count(${settings.id_column}) total from ${tableName}${whereStatement}`;
		console.log(`Range Query: ${query}`);
		connection.query(query, (err, result) => {
			if (err) {
				console.error("Range Error", err);
				callback(err);
			} else {
				callback(null, {
					min: result[0].min,
					max: result[0].max,
					total: result[0].total
				});
			}
		});
	}

	function nibble(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		let query = `select ${settings.id_column} as id from ${tableName}
			where id >= ${escape(data.start)} and id <= ${escape(data.end)}
			order by id ${!data.reverse ? "asc":"desc"}
			limit 2
			offset ${data.limit - 1}`;

		console.log(`Nibble Query: ${query}`);
		connection.query(query, (err, rows) => {
			if (err) {
				console.error("Nibble Error", err);
				callback(err);
			} else {
				data.current = rows[0] ? rows[0].id : null;
				data.next = rows[1] ? rows[1].id : null;
				callback(null, data);
			}
		});
	}

	function initialize(event, callback) {
		// Generate session data
		let session = {
			id: uuid.v4()
		};
		if (typeof event.settings.table == "object" && event.settings.table.sql) {
			session.table = `${event.settings.table.name || 'leo_chk'}_${moment.now()}`;

			console.log("Table", session.table);

			var connection = getConnection(event.settings);
			connection.query(`create table ${session.table} (${event.settings.table.sql})`, (err) => {
				session.drop = !err;
				err && console.error(err);
				callback(err, session);
			});
		} else {
			callback(null, session);
		}
	}

	function destroy(event, callback) {
		callback();
	}

	function escape(value) {
		if (typeof value == "string") {
			return "'" + value + "'";
		}
		return value;
	}

	function getFields(connection, event) {
		let settings = event.settings;

		if (!event.settings.sql) {
			let tableName = getTable(event);

			if (!event.settings.fields) {
				event.settings.fields = [event.settings.id_column];
			}

			event.settings.sql = `SELECT ${settings.fields.map(field => {
					if (field.match(/^\*/)) {
						return field.slice(1).replace(/[\'\"\`]/g, '');
					} else {
						return field.replace(/[\'\"\`]/g, '');
					}
				})}
				FROM ${tableName}
				where ${event.settings.id_column} __IDCOLUMNLIMIT__`;
		}

		return new Promise((resolve, reject) => {
			connection.query(event.settings.sql.replace('__IDCOLUMNLIMIT__', ` between 1 and 0 LIMIT 0`), (err, rows, fields) => {
				if (err) {
					reject(err);
					return;
				}
				console.log(fields);
				resolve({
					sql: event.settings.sql,
					fieldCalcs: fields.map(f => {
						console.log(f);

						if (['date', 'timestamp', 'datetime'].indexOf(fieldIds[f.dataTypeID].toLowerCase()) !== -1) {
							return `coalesce(md5(floor(extract(epoch from ${f.name}))::text), ' ')`;
						}

						return `coalesce(md5(${f.name}::text), ' ')`;
					}),
					fields: fields.map(f => {
						return f.name
					})
				});
			});
		});
	}

	function where(data, settings) {
		let where = "";
		if (data.ids) {
			where = ` in (${data.ids.map(f=>escape(f))})`
		} else if (data.start || data.end) {
			let parts = [];
			if (data.start && data.end) {
				parts.push(` between ${escape(data.start)} and ${escape(data.end)} `);
			} else if (data.start) {
				parts.push(` >= ${escape(data.start)}`);
			} else if (data.end) {
				parts.push(` <= ${escape(data.end)}`);
			}
			where = parts.join(" and ");
		} else {
			where = "1=1";
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

	function getTable(event) {
		let table = event.settings.table;
		return event.session && event.session.table || ((typeof table === "object") ? table.name : table);
	}

	function buildWhere(where, combine) {
		combine = combine || "and";
		if (where) {
			let w = [];
			if (typeof where == "object" && where.length) {
				where.forEach(function(e) {
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
				for (let k in where) {
					let entry = where[k];
					let val = "";
					let op = "=";

					if (typeof(entry) != "object") {
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
		return "";
	}
};

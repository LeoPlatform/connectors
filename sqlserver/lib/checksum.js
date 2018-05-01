"use strict";
const uuid = require("uuid");
const base = require("leo-connector-common/checksum/lib/handler.js");
const moment = require("moment");

let fieldTypes = {
	INT8: 127,
	BIGVARCHR: 167,

	// unverified for sql server
	BIT: 16,
	BLOB: 252,
	DATE: 10,
	DATETIME: 12,
	DECIMAL: 0,
	DOUBLE: 5,
	ENUM: 247,
	FLOAT: 4,
	GEOMETRY: 255,
	INT24: 9,
	LONG: 3,
	LONGLONG: 8,
	LONG_BLOB: 251,
	MEDIUM_BLOB: 250,
	NEWDATE: 14,
	NEWDECIMAL: 246,
	NULL: 6,
	SET: 248,
	SHORT: 2,
	STRING: 254,
	TIME: 11,
	TIMESTAMP: 7,
	TINY: 1,
	TINY_BLOB: 249,
	VARCHAR: 15,
	VAR_STRING: 253,
	YEAR: 13
};
let fieldIds = {};
Object.keys(fieldTypes).forEach(key => {
	fieldIds[fieldTypes[key]] = key;
});

module.exports = function (connection) {
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
			let fieldCalcConcat = table.fieldCalcs;

			if (table.fieldCalcs.length > 1) {
				fieldCalcConcat = `CONCAT(${table.fieldCalcs.join(', ')})`;
			}
			let batchQuery = `SELECT COUNT(*) AS count,
					SUM(CONVERT(BIGINT, CONVERT(VARBINARY, SUBSTRING(hash, 1, 8), 2))) AS sum1,
					SUM(CONVERT(BIGINT, CONVERT(VARBINARY, SUBSTRING(hash, 9, 8), 2))) AS sum2,
					SUM(CONVERT(BIGINT, CONVERT(VARBINARY, SUBSTRING(hash, 17, 8), 2))) AS sum3,
					SUM(CONVERT(BIGINT, CONVERT(VARBINARY, SUBSTRING(hash, 25, 8), 2))) AS sum4
				FROM (
					SELECT LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', ${fieldCalcConcat}), 2)) AS hash
					FROM (${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}) i
				) AS t`;

			console.log("Batch Query", batchQuery);
			connection.query(batchQuery, (err, rows) => {
				if (err) {
					console.log("Batch Checksum Error", err);
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
		}).catch(callback); //.then(() => connection.end());
	}

	function individual(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let fieldCalcConcat = table.fieldCalcs;

			if (table.fieldCalcs.length > 1) {
				fieldCalcConcat = `CONCAT(${table.fieldCalcs.join(', ')})`;
			}

			let individualQuery = `SELECT ${settings.id_column} AS id, LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', ${fieldCalcConcat}), 2)) AS hash
				FROM (${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}) i`;

			console.log("Individual Query", individualQuery);
			connection.query(individualQuery, (err, rows) => {
				if (err) {
					console.log("Individual Checksum Error", err);
					callback(err);
				} else {
					let results = {
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
		}).catch(callback); //.then(() => connection.end());
	}

	function del(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let delQuery = `DELETE from ${tableName}
				WHERE ${settings.id_column} IN (${data.ids.map(f => escape(f))})`;

			console.log("Delete Query", delQuery);
			connection.query(delQuery, (err, rows) => {
				if (err) {
					console.log("Delete Error", err);
					callback(err);
					return;
				}
				let results = {
					ids: data.ids,
				};
				callback(null, results);
			});
		}).catch(callback); //.then(() => connection.end());
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
		}).catch(callback); //.then(() => connection.end());
	}

	function range(event, callback) {
		//console.log("Calling Range", event);

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
		let query = `SELECT MIN(${settings.id_column}) AS min, MAX(${settings.id_column}) AS max, COUNT(${settings.id_column}) AS total FROM ${tableName}${whereStatement}`;
		console.log(`Range Query: ${query}`);
		connection.query(query, (err, result) => {
			if (err) {
				console.log("Range Error", err);
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

		let query = `SELECT ${settings.id_column} AS id
			FROM ${tableName}
			WHERE id >= ${escape(data.start)} AND id <= ${escape(data.end)}
			ORDER BY id ${!data.reverse ? "ASC" : "DESC"}
			OFFSET ${data.limit - 1} ROWS
			FETCH NEXT 2 ROWS ONLY`;

		console.log(`Nibble Query: ${query}`);
		connection.query(query, (err, rows) => {
			if (err) {
				console.log("Nibble Error", err);
				callback(err);
			} else {
				data.current = rows[0] ? rows[0].id : null;
				data.next = rows[1] ? rows[1].id : null;
				callback(null, data)
			}
		});
	}

	function initialize(event, callback) {
		// Generate session data
		let session = {
			id: uuid.v4()
		};
		if (typeof event.settings.table === "object" && event.settings.table.sql) {
			let connection = getConnection(event.settings);
			session.table = `${event.settings.table.name || 'leo_chk'}_${moment.now()}`;

			console.log("Table", session.table);

			connection.query(`create table ${session.table} (${event.settings.table.sql})`, (err, data) => {
				session.drop = !err;
				console.log(err);
				callback(err, session)
			});
		} else {
			callback(null, session);
		}
	}

	function destroy(event, callback) {
		callback();
	}

	function escape(value) {
		if (typeof value === "string") {
			return "'" + value + "'";
		}
		return value;
	}

	function getFields(connection, event) {
		if (!event.settings.sql) {
			let tableName = getTable(event);

			if (!event.settings.fields) {
				event.settings.fields = [event.settings.id_column];
			}

			event.settings.sql = `SELECT ${event.settings.fields.map(field => {
					if (field.match(/^\*/)) {
						return field.slice(1).replace(/[\'\"\`]/g, '');
					} else {
						return field.replace(/[\'\"\`]/g, '');
					}
				})}
				FROM ${tableName}
				WHERE ${event.settings.id_column} __IDCOLUMNLIMIT__`;
		}

		return new Promise((resolve, reject) => {
			connection.query(event.settings.sql.replace('__IDCOLUMNLIMIT__', ` BETWEEN 1 AND 0`), (err, rows, fields) => {
				if (err) {
					reject(err);
					return;
				}

				resolve({
					sql: event.settings.sql,
					fieldCalcs: fields.map(f => {
						console.log(f);
						if (['date', 'timestamp', 'datetime'].indexOf(fieldIds[f.type.id].toLowerCase()) !== -1) {
							return `COALESCE(LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', FLOOR(CAST(DATEDIFF(s, '19700101', cast('${f.name}' AS datetime)) AS bigint))), 2)), " ")`;
						}

						return `LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', CONVERT(VARCHAR(32), ${f.name})), 2))`;
					}),
					fields: fields.map(f => {
						return f.name
					})
				});
			}, {inRowMode: true});
		});
	}

	function where(data, settings) {
		let where = "";
		if (data.ids) {
			where = ` IN (${data.ids.map(f => escape(f))})`
		} else if (data.start || data.end) {
			let parts = [];
			if (data.start && data.end) {
				parts.push(` BETWEEN ${escape(data.start)} AND ${escape(data.end)} `);
			} else if (data.start) {
				parts.push(` >= ${escape(data.start)}`);
			} else if (data.end) {
				parts.push(` <= ${escape(data.end)}`);
			}
			where = parts.join(" AND ");
		} else {
			where = "1 = 1";
		}

		if (settings.where) {
			if (where.trim() != '') {
				where += " AND ";
			} else {
				where = "WHERE ";
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
			if (typeof where === "object" && where.length) {
				where.forEach(function (e) {
					if (typeof e !== "object") {
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
			} else if (typeof where === "object") {
				for (let entry of where) {
					let val = "";
					let op = "=";
					let k = '';

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

			let joined = w.join(` ${combine} `);
			return `(${joined})`;
		}
		return ""
	}
};
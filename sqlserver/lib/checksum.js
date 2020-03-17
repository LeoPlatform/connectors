"use strict";
const uuid = require("uuid");
const base = require("leo-connector-common/checksum/lib/handler.js");
const moment = require("moment");
const logger = require('leo-logger')('sqlserver-checksum-api');

let fieldTypes = {
	INT8: 127,
	BIGVARCHR: 167,
	BIGINT: 38
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

			logger.log("Batch Query", batchQuery);
			connection.query(batchQuery, (err, rows) => {
				if (err) {
					logger.log("Batch Checksum Error", err);
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
			}, {inRowMode: false});
		}).catch(callback);
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

			logger.log("Individual Query", individualQuery);
			connection.query(individualQuery, (err, rows) => {
				if (err) {
					logger.log("Individual Checksum Error", err);
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
			}, {inRowMode: false});
		}).catch(callback);
	}

	function del(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let delQuery = `DELETE from ${tableName}
				WHERE ${settings.id_column} IN (${data.ids.map(f => escape(f))})`;

			logger.log("Delete Query", delQuery);
			connection.query(delQuery, (err) => {
				if (err) {
					logger.log("Delete Error", err);
					callback(err);
					return;
				}
				let results = {
					ids: data.ids,
				};
				callback(null, results);
			}, {inRowMode: false});
		}).catch(callback);
	}

	function sample(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			let sampleQuery = table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings));
			logger.log("Sample Query", sampleQuery);
			connection.query(sampleQuery, (err, rows) => {
				if (err) {
					logger.log("Sample Error", err);
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
			}, {inRowMode: false});
		}).catch(callback);
	}

	function range(event, callback) {
		logger.log("Calling Range", event);

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		let wheres = [];
		let whereStatement = "";
		if (data.min) {
			wheres.push(`${settings.id_column} >= ${escape(data.min)}`);
		}
		if (data.max) {
			wheres.push(`${settings.id_column} <= ${escape(data.max)}`);
		}
		if (wheres.length) {
			whereStatement = ` where ${wheres.join(" and ")} `;
		}
		getFields(connection, event).then((table) => {
			let query = `SELECT MIN(${settings.id_column}) AS min, MAX(${settings.id_column}) AS max, COUNT(${settings.id_column}) AS total `;
			if (!table.sql) {
				query += `FROM ${tableName}${whereStatement}`;
			} else {
				query += `FROM (${table.sql.replace('__IDCOLUMNLIMIT__', ' IS NOT NULL AND ' + where(data, settings))}) i ${whereStatement}`;
			}
			logger.log(`Range Query: ${query}`);
			connection.query(query, (err, result, fields) => {
				if (err) {
					logger.log("Range Error", err);
					callback(err);
				} else {
					callback(null, {
						min: correctValue(result[0][0], fields[0]),
						max: correctValue(result[0][1], fields[1]),
						total: correctValue(result[0][2], fields[2])
					});
				}
			}, {inRowMode: true});
		}).catch(callback);
	}

	function nibble(event, callback) {

		let data = event.data;
		let settings = event.settings;
		let tableName = getTable(event);
		let connection = getConnection(settings);

		let query = `SELECT ${settings.id_column} AS id
			FROM ${tableName}
			WHERE ${settings.id_column} >= ${escape(data.start)} AND ${settings.id_column} <= ${escape(data.end)}
			ORDER BY id ${!data.reverse ? "ASC" : "DESC"}
			OFFSET ${data.limit - 1} ROWS
			FETCH NEXT 2 ROWS ONLY`;

		logger.log(`Nibble Query: ${query}`);
		connection.query(query, (err, rows, fields) => {
			if (err) {
				logger.log("Nibble Error", err);
				callback(err);
			} else {
				// logger.log('query', query);
				// logger.log(rows, fields);
				data.current = rows[0] ? correctValue(rows[0][0], fields[0]) : null;
				data.next = rows[1] ? correctValue(rows[1][0], fields[0]) : null;
				callback(null, data)
			}
		}, {inRowMode: true});
	}

	function initialize(event, callback) {
		// Generate session data
		let session = {
			id: uuid.v4()
		};
		if (typeof event.settings.table === "object" && event.settings.table.sql) {
			let connection = getConnection(event.settings);
			session.table = `${event.settings.table.name || 'leo_chk'}_${moment.now()}`;

			logger.log("Table", session.table);

			connection.query(`create table ${session.table} (${event.settings.table.sql})`, (err, data) => {
				session.drop = !err;
				logger.log(err);
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

		return new Promise((resolve, reject) => {
			if (!event.settings.fields && !event.settings.sql) {
				reject("Missing required object parameter: 'sql', in the SQL Server lambdaConnector.");
			} else if (!event.settings.sql) {
				let tableName = getTable(event);

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

			connection.query(event.settings.sql.replace('__IDCOLUMNLIMIT__', ` BETWEEN 1 AND 0`), (err, rows, fields) => {
				if (err) {
					reject(err);
					return;
				}
				resolve({
					sql: event.settings.sql,
					fieldCalcs: fields.map(f => {
						if (fieldIds[f.type.id] && ['date', 'timestamp', 'datetime'].indexOf(fieldIds[f.type.id].toLowerCase()) !== -1) {
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

	/**
	 * Convert bigint from string to integer for comparisons
	 * @param value
	 * @param metadata
	 * @returns {*}
	 */
	function correctValue(value, metadata) {
		switch (metadata.type.id) {
			case 38:
			case 127:
				return parseInt(value);
		}

		return value;
	}
};

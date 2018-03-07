"use strict";
var aws = require("aws-sdk");
const crypto = require('crypto');
var uuid = require("uuid");
var base = require("../checksum/index.js");
var mysql = require('mysql');
var moment = require("moment");



let fieldTypes = {
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
	})


	function getConnection(settings) {
		return connection;
	}

	function batch(event, callback) {
		var startTime = moment.now();

		var data = event.data;
		var settings = event.settings;
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fieldCalcs = table.fieldCalcs;
			var batchQuery = `
		select count(*) as count,
		sum(truncate(conv(substring(hash, 1, 8), 16, 10), 0)) as sum1,
		sum(truncate(conv(substring(hash, 9, 8), 16, 10), 0)) as sum2,
		sum(truncate(conv(substring(hash, 17, 8), 16, 10), 0)) as sum3,
		sum(truncate(conv(substring(hash, 25, 8), 16, 10), 0)) as sum4
		from (
			select md5(concat(${fieldCalcs.join(', ')})) as hash
			from (
        ${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}
        order by ${settings.id_column} asc
      ) i
		) as t
		`;
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

		//console.log("Calling Individual");
		var startTime = moment.now();
		var tableName = getTable(event);

		var data = event.data;
		var settings = event.settings
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fieldCalcs = table.fieldCalcs;
			var individualQuery = `
      select ${settings.id_column} as id, md5(concat(${fieldCalcs.join(', ')})) as hash
      from (
        ${table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings))}
        order by ${settings.id_column} asc
      ) i
		`;
			console.log("Individual Query", individualQuery);
			connection.query(individualQuery, (err, rows) => {
				//connection.end();
				if (err) {
					console.log("Individual Checksum Error", err);
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
						console.log(row)
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

		//console.log("Calling Sample", event);
		var data = event.data;
		var settings = event.settings
		var tableName = getTable(event);
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fields = table.fields;
			var delQuery = `
    delete from ${tableName}
      where ${settings.id_column} in (${data.ids.map(f=>escape(f))})`;

			console.log("Delete Query", delQuery);
			connection.query(delQuery, (err, rows) => {
				if (err) {
					console.log("Delete Error", err)
					callback(err);
					return;
				}
				var results = {
					ids: data.ids,
				};
				callback(null, results);
			});
		}).catch(callback); //.then(() => connection.end());
	}

	function sample(event, callback) {

		//console.log("Calling Sample", event);
		var data = event.data;
		var settings = event.settings
		var tableName = getTable(event);
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fields = table.fields;
			var sampleQuery = table.sql.replace('__IDCOLUMNLIMIT__', where(data, settings)) + ` order by ${settings.id_column} asc`;
			console.log("Sample Query", sampleQuery);
			connection.query(sampleQuery, (err, rows) => {
				if (err) {
					console.log("Sample Error", err)
					callback(err);
					return;
				}
				var results = {
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

		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		var where = [];
		var whereStatement = "";
		if (data.min) {
			where.push(`${settings.id_column} >= ${escape(data.min)}`);
		}
		if (data.max) {
			where.push(`${settings.id_column} <= ${escape(data.max)}`);
		}
		if (where.length) {
			whereStatement = ` where ${where.join(" and ")} `;
		}
		var query = `select MIN(${settings.id_column}) as min, MAX(${settings.id_column}) as max, count(${settings.id_column}) total from ${tableName}${whereStatement}`;
		console.log(`Range Query: ${query}`);
		connection.query(query, (err, result) => {
			//connection.end();
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
		//console.log("Calling Nibble", event);

		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		var query = `
		select ${settings.id_column} as id from ${tableName} force key(${settings.key_column || settings.id_column})
		where id >= ${escape(data.start)} and id <= ${escape(data.end)}
		order by id ${!data.reverse ? "asc":"desc"}
		limit 2
		offset ${data.limit - 1}
	`;
		console.log(`Nibble Query: ${query}`);
		connection.query(query, (err, rows) => {
			//connection.end();
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
		//console.log("Calling Initialize", event)
		// Generate session data

		var session = {
			id: uuid.v4()
		};
		if (typeof event.settings.table == "object" && event.settings.table.sql) {
			session.table = `${event.settings.table.name || 'leo_chk'}_${moment.now()}`;
			console.log("Table", session.table)
			var connection = getConnection(event.settings);
			connection.query(`create table ${session.table} (${event.settings.table.sql})`, (err, data) => {
				//connection.end();
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
		if (typeof value == "string") {
			return "'" + value + "'";
		}
		return value;
	}

	function escapeId(value) {
		if (typeof value == "string") {
			return "`" + value + "`";
		}
		return value;
	}

	function getFields(connection, event) {
		var settings = event.settings;
		if (!event.settings.sql) {
			var tableName = getTable(event);
			event.settings.sql = `
      SELECT ${settings.fields.map(field => {
        if(field.match(/^\*/)) {
          return escapeId(field.slice(1));
        } else {
          return escapeId(field);
        }
      })}
      FROM ${tableName}
      where ${event.settings.id_column} __IDCOLUMNLIMIT__
    `;
		}
		return new Promise((resolve, reject) => {
			connection.query(event.settings.sql.replace('__IDCOLUMNLIMIT__', ` between 1 and 0`), (err, rows, fields) => {
				if (err) {
					reject(err);
					return;
				}
				resolve({
					sql: event.settings.sql,
					fieldCalcs: fields.map(f => {
						if (['date', 'timestamp', 'datetime'].indexOf(fieldIds[f.type].toLowerCase()) !== -1) {
							return `coalesce(md5(floor(UNIX_TIMESTAMP(\`${f.name}\`))), " ")`
						} else {
							return `coalesce(md5(\`${f.name}\`), " ")`;
						}
						return escapeId(f.name)
					}),
					fields: fields.map(f => {
						return f.name
					})
				});
			});
		});
	}

	function where(data, settings) {
		var where = "";
		if (data.ids) {
			where = ` in (${data.ids.map(f=>escape(f))})`
		} else if (data.start || data.end) {
			var parts = [];
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
		var table = event.settings.table;
		return event.session.table || ((typeof table === "object") ? table.name : table);
	}


	function buildWhere(where, combine) {
		combine = combine || "and"
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

					if (typeof (entry) != "object") {
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
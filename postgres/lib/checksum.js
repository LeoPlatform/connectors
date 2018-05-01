"use strict";
var base = require("leo-connector-common/checksum/lib/handler.js");
var moment = require("moment");
var dynamodb = require("leo-sdk").aws.dynamodb;
var logger = require("leo-sdk/lib/logger")("postgres-checksum-api");

module.exports = function(connection, fieldsTable) {
	if (!connection.end && connection.disconnect) {
		connection.end = connection.disconnect;
	}

	var FIELDS_TABLE = fieldsTable;

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

	function wrap(method) {
		return function(event, callback) {
			method(event, (err, data) => {
				connection.end();
				callback(err, data);
			});
		};
	}

	function getConnection() {
		return connection;
	}

	function batch(event, callback) {
		//logger.log("Calling Batch", event);

		var startTime = moment.now();
		var tableName = getTable(event);

		var data = event.data;
		var settings = event.settings;

		//var fields = settings.fields;
		var connection = getConnection(settings);
		getFields(connection, event).then((table) => {
			var fieldCalcs = table.fieldCalcs;
			var batchQuery = `
			select count(*) as count,
				cast(sum(trunc(strtol(substring(hash, 1, 8), 16))) as decimal) as sum1,
				cast(sum(trunc(strtol(substring(hash, 9, 8), 16))) as decimal) as sum2,
				cast(sum(trunc(strtol(substring(hash, 17, 8), 16))) as decimal) as sum3,
				cast(sum(trunc(strtol(substring(hash, 25, 8), 16))) as decimal) as sum4
			from (
				select md5(${fieldCalcs.join(' || ')}) as "hash"
				from "${tableName}"
				${where(data, settings)}
				order by LOWER("${settings.id_column}") asc
			) as t
			`;

			logger.log("Batch Query", batchQuery);
			connection.query(batchQuery, (err, rows) => {
				//connection.end();
				if (err) {
					logger.error("Batch Checksum Error", err);
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
		//logger.log("Calling Individual");
		var tableName = getTable(event);

		var data = event.data;
		var settings = event.settings;
		//var fields = settings.fields;
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fieldCalcs = table.fieldCalcs;
			var individualQuery = `
				select "${settings.id_column}" as id, md5(${fieldCalcs.join(' || ')}) as "hash"
				from "${tableName}"
				${where(data, settings)}
				order by LOWER("${settings.id_column}") asc
			`;

			logger.log("Individual Query", individualQuery);
			connection.query(individualQuery, (err, rows) => {
				//connection.end();
				if (err) {
					logger.error("Individual Checksum Error", err);
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

		//logger.log("Calling Sample", event);
		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		//getFields(connection, event).then((table) => {
		var delQuery = `
    delete from ${tableName}
      where ${settings.id_column} in (${data.ids.map(f=>escape(f))})`;

		logger.log("Delete Query", delQuery);
		connection.query(delQuery, (err) => {
			if (err) {
				logger.error("Delete Error", err);
				callback(err);
				return;
			}
			var results = {
				ids: data.ids,
			};
			callback(null, results);
		});
		//}).catch(callback); //.then(() => connection.end());
	}

	function sample(event, callback) {
		//logger.log("Calling Sample", event);
		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		getFields(connection, event).then((table) => {
			var fields = table.fields;
			var sampleQuery = `
			select "${settings.id_column}" as id,
				${fields.map((f,i)=>`"${f}" as _${i}`).join(",")}
			from "${tableName}"
			${where(data, settings)}
			order by LOWER("${settings.id_column}") asc`;

			logger.log("Sample Query", sampleQuery);
			connection.query(sampleQuery, (err, rows) => {
				if (err) {
					logger.error("Sample Error", err);
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
							return row[f];
						}).slice(1);
					})
				};
				callback(null, results);
			});
		}).catch(callback);

	}

	function range(event, callback) {
		//logger.log("Calling Range", event);

		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		var where = [];
		var whereStatement = "";
		if (data.min) {
			where.push(`"${settings.id_column}" >= ${escape(data.min)}`);
		}
		if (data.max) {
			where.push(`"${settings.id_column}" <= ${escape(data.max)}`);
		}
		if (where.length) {
			whereStatement = ` where ${where.join(" and ")} `;
		}
		var query = `select MIN("${settings.id_column}") as min, MAX("${settings.id_column}") as max, count("${settings.id_column}") total from "${tableName}"${whereStatement}`;
		logger.log(`Range Query: ${query}`);
		connection.query(query, (err, result) => {
			//connection.end();
			if (err) {
				logger.error("Range Error", err);
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
		//logger.log("Calling Nibble", event);

		var data = event.data;
		var settings = event.settings;
		var tableName = getTable(event);
		var connection = getConnection(settings);

		var query = `
			select "${settings.id_column}" as id from ${tableName}
			where id >= ${escape(data.start)} and id <= ${escape(data.end)}
			order by id ${!data.reverse ? "asc":"desc"}
			limit 2
			offset ${data.limit - 1}
		`;
		logger.log(`Nibble Query: ${query}`);
		connection.query(query, (err, rows) => {
			//connection.end();
			if (err) {
				logger.error("Nibble Error", err);
				callback(err);
			} else {
				data.current = rows[0] ? rows[0].id : null;
				data.next = rows[1] ? rows[1].id : null;
				callback(null, data);
			}
		});
	}

	function initialize(event, callback) {
		//logger.log("Calling Initialize", event)
		// Generate session data
		var session = event.session;

		if (typeof event.settings.table == "object" && event.settings.table.sql) {
			session.table = `${event.settings.table.name || 'leo_chk'}_${moment.now()}`;
			logger.log("Table", session.table);
			var connection = getConnection(event.settings);
			connection.query(`create table ${session.table} (${event.settings.table.sql})`, (err) => {
				//connection.end();
				session.drop = !err;
				err && logger.error(err);
				callback(err, session);
			});
		} else {
			var connection = getConnection(event.settings);
			let tableName = getTable(event);
			session.sql = event.settings.sql;
			if (!event.settings.sql) {
				session.sql = `select ${event.settings.fields.join(', ')}
				from "${tableName}"
				WHERE ${event.settings.id_column} __IDCOLUMNLIMIT__
				`;
			}
			connection.query(`${session.sql.replace('__IDCOLUMNLIMIT__', ' between 1 and 0')} LIMIT 0`, (err, results, fields) => {
				let rFields = [];
				console.log(fields);
				fields.forEach((f, i) => {
					let field = {
						column: event.settings.fields ? event.settings.fields[i] : f.name,
						type_id: f.dataTypeID,
						type: types[f.dataTypeID]
					};
					if (field.type.match(/date/) || field.type.match(/time/)) {
						field.sql = `coalesce(md5(floor(extract(epoch from ${field.column}))::text), ' ')`;
					} else {
						field.sql = `coalesce(md5((${field.column})::text), ' ')`;
					}
					rFields.push(field);
				});
				//callback(null, {
				session.fields = rFields;
				session.tableName = tableName;
				session.idColumn = event.settings.id_column || event.settings.idColumn;
				session.sql = session.sql;
				//});
				callback();
			});

			//callback(null, session);
		}
	}

	function destroy(event, callback) {
		callback();
	}

	function getTable(event) {
		var table = event.settings.table || event.settings.tableName;
		return event.session && event.session.table || ((typeof table === "object") ? table.name : table);
	}

	function escape(value) {
		if (typeof value == "string") {
			return "'" + value + "'";
		}
		return value;
	}

	function getFields(connection, event) {
		var settings = event.settings;
		var tableName = getTable(event);
		return new Promise((resolve, reject) => {
			let doCall = (done) => {
				dynamodb.get(FIELDS_TABLE, tableName, {
					id: "identifier"
				}, done);
			};
			if (!FIELDS_TABLE) {
				doCall = (done) => done(null, {
					structure: event.session.fields.reduce((sum, f) => {
						sum[f.column] = f;
						return sum;
					}, {})
				});
			} else if (typeof FIELDS_TABLE === "object") {
				doCall = (done) => done(null, {
					structure: FIELDS_TABLE
				});
			}
			doCall(function(err, table) {

				if (err) {
					logger.error("Batch Get Types Error", err);
					reject(err);
				} else {
					if (!table.structure) {
						table.structure = {};
						Object.keys(table.fields).map(k => {
							let field = table.fields[k];

							table.structure[field.column] = {
								type: field.type || field.dtype
							};
						});
					}

					var origIdColumn = '"' + settings.id_column + '"';
					var idColumn = origIdColumn;

					if (table.structure[settings.id_column].type == "string") {
						idColumn = 'LOWER(' + idColumn + ')';
					}
					var startTime = moment.now();
					var fields = [];
					var fieldCalcs = settings.fields.map((f) => {
						if (!(f in table.structure)) {
							logger.info("missing field", f);
							return "' '";
						}
						var type = table.structure[f].type;
						var column = f;
						fields.push(column);
						switch (type) {
							case 'string':
								return `coalesce(md5("${column}"::text), ' ')`;
							case 'int':
								return `coalesce(md5("${column}"::text), ' ')`;
							case 'date':
							case 'datetime':
								return `coalesce(md5(floor(date_part(epoch, "${column}"))::text), ' ')`;
							default:
								return table.structure[f].sql || `coalesce(md5("${column}"::text), ' ')`;
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
			where = `where "${settings.id_column}" in (${data.ids.map(f=>escape(f))})`;
		} else if (data.start || data.end) {
			var parts = [];
			if (data.start) {
				parts.push(`"${settings.id_column}" >= ${escape(data.start)}`);
			}
			if (data.end) {
				parts.push(`"${settings.id_column}" <= ${escape(data.end)}`);
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
		return (settings.join ? `join ${settings.join}\n` : "") + where;
	}

	function buildWhere(where, combine) {
		combine = combine || "and";
		if (where) {
			var w = [];
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
				for (var k in where) {
					var entry = where[k];
					var val = "";
					var op = "=";

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
let types = {
	16: 'bool',
	17: 'bytea',
	18: 'char',
	19: 'name',
	20: 'int8',
	21: 'int2',
	22: 'int2vector',
	23: 'int4',
	24: 'regproc',
	25: 'text',
	26: 'oid',
	27: 'tid',
	28: 'xid',
	29: 'cid',
	30: 'oidvector',
	71: 'pg_type',
	75: 'pg_attribute',
	81: 'pg_proc',
	83: 'pg_class',
	114: 'json',
	142: 'xml',
	143: '_xml',
	199: '_json',
	194: 'pg_node_tree',
	32: 'pg_ddl_command',
	210: 'smgr',
	600: 'point',
	601: 'lseg',
	602: 'path',
	603: 'box',
	604: 'polygon',
	628: 'line',
	629: '_line',
	700: 'float4',
	701: 'float8',
	702: 'abstime',
	703: 'reltime',
	704: 'tinterval',
	705: 'unknown',
	718: 'circle',
	719: '_circle',
	790: 'money',
	791: '_money',
	829: 'macaddr',
	869: 'inet',
	650: 'cidr',
	1000: '_bool',
	1001: '_bytea',
	1002: '_char',
	1003: '_name',
	1005: '_int2',
	1006: '_int2vector',
	1007: '_int4',
	1008: '_regproc',
	1009: '_text',
	1028: '_oid',
	1010: '_tid',
	1011: '_xid',
	1012: '_cid',
	1013: '_oidvector',
	1014: '_bpchar',
	1015: '_varchar',
	1016: '_int8',
	1017: '_point',
	1018: '_lseg',
	1019: '_path',
	1020: '_box',
	1021: '_float4',
	1022: '_float8',
	1023: '_abstime',
	1024: '_reltime',
	1025: '_tinterval',
	1027: '_polygon',
	1033: 'aclitem',
	1034: '_aclitem',
	1040: '_macaddr',
	1041: '_inet',
	651: '_cidr',
	1263: '_cstring',
	1042: 'bpchar',
	1043: 'varchar',
	1082: 'date',
	1083: 'time',
	1114: 'timestamp',
	1115: '_timestamp',
	1182: '_date',
	1183: '_time',
	1184: 'timestamptz',
	1185: '_timestamptz',
	1186: 'interval',
	1187: '_interval',
	1231: '_numeric',
	1266: 'timetz',
	1270: '_timetz',
	1560: 'bit',
	1561: '_bit',
	1562: 'varbit',
	1563: '_varbit',
	1700: 'numeric',
	1790: 'refcursor',
	2201: '_refcursor',
	2202: 'regprocedure',
	2203: 'regoper',
	2204: 'regoperator',
	2205: 'regclass',
	2206: 'regtype',
	4096: 'regrole',
	4089: 'regnamespace',
	2207: '_regprocedure',
	2208: '_regoper',
	2209: '_regoperator',
	2210: '_regclass',
	2211: '_regtype',
	4097: '_regrole',
	4090: '_regnamespace',
	2950: 'uuid',
	2951: '_uuid',
	3220: 'pg_lsn',
	3221: '_pg_lsn',
	3614: 'tsvector',
	3642: 'gtsvector',
	3615: 'tsquery',
	3734: 'regconfig',
	3769: 'regdictionary',
	3643: '_tsvector',
	3644: '_gtsvector',
	3645: '_tsquery',
	3735: '_regconfig',
	3770: '_regdictionary',
	3802: 'jsonb',
	3807: '_jsonb',
	2970: 'txid_snapshot',
	2949: '_txid_snapshot',
	3904: 'int4range',
	3905: '_int4range',
	3906: 'numrange',
	3907: '_numrange',
	3908: 'tsrange',
	3909: '_tsrange',
	3910: 'tstzrange',
	3911: '_tstzrange',
	3912: 'daterange',
	3913: '_daterange',
	3926: 'int8range',
	3927: '_int8range',
	2249: 'record',
	2287: '_record',
	2275: 'cstring',
	2276: 'any',
	2277: 'anyarray',
	2278: 'void',
	2279: 'trigger',
	3838: 'event_trigger',
	2280: 'language_handler',
	2281: 'internal',
	2282: 'opaque',
	2283: 'anyelement',
	2776: 'anynonarray',
	3500: 'anyenum',
	3115: 'fdw_handler',
	325: 'index_am_handler',
	3310: 'tsm_handler',
	3831: 'anyrange'
};
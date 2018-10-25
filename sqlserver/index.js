"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum.js");
const logger = require("leo-sdk/lib/logger")("sqlserver");
const PassThrough = require("stream").PassThrough;

const parent = require('leo-connector-common/base');

class connector extends parent {
	constructor() {
		super();
		super.lib_connect = connect;
		super.lib_checksum = checksum;
	}

	streamChanges(config, tables, opts = {}) {
		let client = connect(config);

		let stream = new PassThrough({
			objectMode: true
		});
		let obj = {
			payload: {},
			correlation_id: {
				source: opts.source || "system:sqlserver",
				start: ``,
				units: 0
			}
		};

		if (opts.start === undefined) {
			console.error(`Start is a required parameter`);
			process.exit();
		}

		let parts = opts.start.toString().split(".");
		let version = parseInt(parts.shift());
		let order = '';

		let sqlTables = Object.keys(tables).map(t => {
			let fields;
			let where;

			// build fields for composite keys
			if (Array.isArray(tables[t])) {
				let count = 0;
				let next = `SYS_CHANGE_VERSION = ${version}`;
				let queryPieces = [];

				tables[t].forEach(field => {
					queryPieces.push(`(${next} AND ${field} > ${parts[count]})`);
					next += ` AND ${field} = ${parts[count++]}`;
				});

				where = ' OR ' + queryPieces.join(' OR ');
				fields = tables[t].join(', ');
				order = order || tables[t].join(' asc,') + ' asc';
			} else {
				where = ` OR (SYS_CHANGE_VERSION = ${version} AND ${tables[t]} > ${parts[0] || 0})`;
				fields = tables[t];
				order = order || tables[t] + ' asc';
			}

			let query = `SELECT '${t}' as tableName, ${fields}, SYS_CHANGE_VERSION __SYS_CHANGE_VERSION
				FROM  CHANGETABLE(CHANGES ${t}, ${Math.max(version - 1, 0)}) AS CT
				where SYS_CHANGE_VERSION > ${version}${where}`;

			logger.log(query);
			return query;
		});

		let changeQuery = sqlTables.join(" UNION ") + ` order by SYS_CHANGE_VERSION asc, ${order}`;
		client.query(changeQuery, (err, result) => {
			logger.log(changeQuery);
			if (!err) {
				result.forEach(r => {
					let tableName = r.tableName;
					let sys_change_version = r.__SYS_CHANGE_VERSION;
					delete r.tableName;
					delete r.__SYS_CHANGE_VERSION;

					if (!obj.payload[tableName]) {
						obj.payload[tableName] = [];
					}

					let eid = `${sys_change_version}.`;
					let payload;
					if (Object.keys(r).length > 1) {
						eid += tables[tableName].map(field => r[field]).join('.');
						payload = r;
					} else {
						eid += r[tables[tableName]];
						payload = r[tables[tableName]];
					}

					obj.correlation_id.units++;
					obj.correlation_id.start = obj.correlation_id.start || eid;
					obj.correlation_id.end = eid;
					obj.eid = eid;
					obj.payload[tableName].push(payload);
				});

				if (obj.correlation_id.units > 0) {
					stream.write(obj);
				}
			} else {
				console.log(err);
			}

			stream.end();
		}, {inRowMode: false});

		return stream;
	}
}

module.exports = new connector;

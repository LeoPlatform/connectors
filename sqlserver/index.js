"use strict";
const connect = require("./lib/connect.js");
const logger = require("leo-logger")("sqlserver");
const PassThrough = require("stream").PassThrough;
const checksum = require('./lib/checksum');

let modules = {
    checksum,
	streamChanges: async (config, opts = {}) => {
		let client = await connect(config);

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
			throw new Error(`Start is a required parameter`);
		} else if (!opts.table && !opts.tables) {
			throw new Error('Table is a required parameter');
		}

		let parts = opts.start.toString().split(".");
		let version = parseInt(parts.shift());
		let order = '';

		let sqlTables = Object.keys(opts.tables).map(t => {
			let fields;
			let where;

			// build fields for composite keys
			if (Array.isArray(opts.tables[t])) {
				let count = 0;
				let next = `SYS_CHANGE_VERSION = ${version}`;
				let queryPieces = [];

				opts.tables[t].forEach(field => {
					queryPieces.push(`(${next} AND ${field} > ${parts[count]})`);
					next += ` AND ${field} = ${parts[count++]}`;
				});

				where = ' OR ' + queryPieces.join(' OR ');
				fields = opts.tables[t].join(', ');
				order = order || opts.tables[t].join(' asc,') + ' asc';
			} else {
				where = ` OR (SYS_CHANGE_VERSION = ${version} AND ${opts.tables[t]} > ${parts[0] || 0})`;
				fields = opts.tables[t];
				order = order || opts.tables[t] + ' asc';
			}

			let query = `SELECT '${t}' as tableName, ${fields}, SYS_CHANGE_VERSION __SYS_CHANGE_VERSION
				FROM  CHANGETABLE(CHANGES ${t}, ${Math.max(version - 1, 0)}) AS CT
				where SYS_CHANGE_VERSION > ${version}${where}`;

			logger.log(query);
			return query;
		});

		let changeQuery = sqlTables.join(" UNION ") + ` order by SYS_CHANGE_VERSION asc, ${order}`;
		let result = await client.query(changeQuery);

		logger.debug('Query Result', result);

        result && result.recordset && result.recordset.length && result.recordset.forEach(r => {
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
                eid += opts.tables[tableName].map(field => r[field]).join('.');
                payload = r;
            } else {
                eid += r[opts.tables[tableName]];
                payload = r[opts.tables[tableName]];
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

        stream.end();

		return stream;
	}
};

module.exports = modules;

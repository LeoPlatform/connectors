"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");
const PassThrough = require("stream").PassThrough;
const logger = require("leo-sdk/lib/logger")("sqlserver");

module.exports = {
	load: function (config, sql, domain, opts) {
		return sqlLoader(() => connect(config), sql, domain, opts);
	},
	streamChanges: function (config, tables, opts = {}) {
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

		if (opts.start == undefined) {
			console.error(`Start is a required parameter`)
			process.exit();
		}

		let parts = opts.start.toString().split(".");

		let version = parseInt(parts[0]);
		let offset = parts[1] || 0;
		let limit = opts.limit || 10000;

		let sqlTables = Object.keys(tables).map(t => {
			obj.payload[t] = [];
			let query = `SELECT '${t}' as tableName, ${tables[t]}, SYS_CHANGE_VERSION __SYS_CHANGE_VERSION
					 FROM  CHANGETABLE(CHANGES ${t}, ${version - 1}) AS CT  
					 where (SYS_CHANGE_VERSION <> ${version} OR ${tables[t]} > ${offset})
					 order by SYS_CHANGE_VERSION asc, ${tables[t]} asc`;
			logger.log(query);
			return query;
		});
		client.query(sqlTables.join(" UNION "), (err, result) => {
			if (!err) {
				let offset = 0;
				result.forEach(r => {
					let eid = `${r.__SYS_CHANGE_VERSION}.${r[tables[r.tableName]]}`;
					obj.correlation_id.units++;
					obj.correlation_id.start = obj.correlation_id.start || eid;
					obj.correlation_id.end = eid;
					obj.eid = eid;
					obj.payload[r.tableName].push(r[tables[r.tableName]]);
				});
				//console.log(obj)
				stream.write(obj);
			} else {
				console.log(err);
			}
			stream.end();
		});

		return stream;
	}
};
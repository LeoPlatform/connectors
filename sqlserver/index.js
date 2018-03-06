"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const PassThrough = require("stream").PassThrough;
const logger = require("leo-sdk/lib/logger")("sqlserver");

// require("leo-sdk/lib/logger").configure(/.*/, {
// 	all: true
// });
module.exports = {
	load: function(config, sql, domain, opts) {
		return sqlLoader(() => connect(config), sql, domain, opts);

		// Possible solution if the above doesn't work correctly. I didn't find the below to work correctly, but don't
		// want to destroy it we have a change to test another 5k+ records
		// return sqlLoader(() => connect(config), sql, domain, Object.assign({
		// getEid: (id, obj, stats)=>stats.end.replace(/\..*/, "." + id)
		// }, opts));
	},
	streamChanges: function(config, tables, opts = {}) {
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

		let version = parseInt(parts[0]);
		let offset = parts[1] || 0;

		let sqlTables = Object.keys(tables).map(t => {
			obj.payload[t] = [];
			let query = `SELECT '${t}' as tableName, ${tables[t]} as id, SYS_CHANGE_VERSION __SYS_CHANGE_VERSION
					 FROM  CHANGETABLE(CHANGES ${t}, ${version - 1}) AS CT  
					 where SYS_CHANGE_VERSION > ${version} OR (SYS_CHANGE_VERSION = ${version} AND ${tables[t]} > ${offset})`;
			logger.log(query);
			return query;
		});
		client.query(sqlTables.join(" UNION ") + ' order by SYS_CHANGE_VERSION asc, id asc', (err, result) => {
			logger.log(sqlTables.join(" UNION ") + ' order by SYS_CHANGE_VERSION asc, id asc');
			if (!err) {
				result.forEach(r => {
					let eid = `${r.__SYS_CHANGE_VERSION}.${r.id}`;
					obj.correlation_id.units++;
					obj.correlation_id.start = obj.correlation_id.start || eid;
					obj.correlation_id.end = eid;
					obj.eid = eid;
					obj.payload[r.tableName].push(r.id);
				});
				stream.write(obj);
			} else {
				console.log(err);
			}

			stream.end();
		});

		return stream;
	}
};

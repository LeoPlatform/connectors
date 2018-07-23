"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("leo-connector-common/sql/loader");
const sqlLoaderJoin = require('leo-connector-common/sql/loaderJoinTable');
const sqlNibbler = require("leo-connector-common/sql/nibbler");
const snapShotter = require("leo-connector-common/sql/snapshotter");
const checksum = require("./lib/checksum.js");
const leo = require("leo-sdk");
const ls = leo.streams;
const logger = require("leo-sdk/lib/logger")("sqlserver");
const PassThrough = require("stream").PassThrough;

// require("leo-sdk/lib/logger").configure(/.*/, {
// 	all: true
// });
module.exports = {
	load: function(config, sql, domain, opts, idColumns) {
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(connect(config), idColumns, sql, domain, opts);
		} else {
			return sqlLoader(connect(config), sql, domain, opts);
		}

		// Possible solution if the above doesn't work correctly. I didn't find the below to work correctly, but don't
		// want to destroy it we have a change to test another 5k+ records
		// return sqlLoader(() => connect(config), sql, domain, Object.assign({
		// getEid: (id, obj, stats)=>stats.end.replace(/\..*/, "." + id)
		// }, opts));
	},
	nibble: function(config, table, id, opts) {
		return sqlNibbler(connect(config), table, id, opts);
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
		let version = parseInt(parts.shift());
		let order = '';

		let sqlTables = Object.keys(tables).map(t => {
			let count = 0;
			let changes;
			let fields;
			// build fields for composite keys
			if (typeof tables[t] === 'object') {
				fields = tables[t].join(', ');
				order = tables[t].join(' asc,') + ' asc';
				changes = tables[t].map(field => {
					return `${field} > ${parts[count++] || 0}`;
				}).join(' OR ');
			} else {
				order = tables[t] + ' asc';
				fields = tables[t];
				changes = `${tables[t]} > ${parts[count] || 0}`;
			}

			let query = `SELECT '${t}' as tableName, ${fields}, SYS_CHANGE_VERSION __SYS_CHANGE_VERSION
				FROM  CHANGETABLE(CHANGES ${t}, ${version - 1}) AS CT
				where SYS_CHANGE_VERSION > ${version} OR (SYS_CHANGE_VERSION = ${version} AND (${changes}))`;
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
					if (Object.keys(r).length > 1) {
						eid += tables[tableName].map(field => r[field]).join('.');
					} else {
						eid += r[tables[tableName]];
					}

					obj.correlation_id.units++;
					obj.correlation_id.start = obj.correlation_id.start || eid;
					obj.correlation_id.end = eid;
					obj.eid = eid;

					if (Object.keys(r).length > 1) {
						obj.payload[tableName].push(r);
					} else {
						obj.payload[tableName].push(r[tables[tableName]]);
					}
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
	},
	domainObjectLoader: function(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			snapShotter(bot_id, connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
				event: opts.outQueue
			}, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue);//, {start: opts.start});
			let stats = ls.stats(bot_id, opts.inQueue);

			ls.pipe(stream, stats, this.load(dbConfig, sql, domain, opts, dbConfig.id), leo.load(bot_id, opts.outQueue || dbConfig.table), err => {
				if (err) return callback(err);
				return stats.checkpoint(callback);
			});
		}
	},
	checksum: function(config) {
		return checksum(connect(config));
	},
	connect: connect
};

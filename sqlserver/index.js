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
const dol = require("leo-connector-common/dol");

function DomainObjectLoader(client) {
  if (typeof client.query !== "function") {
    client = connect(client);
  }
  return new dol(client)
}

module.exports = {
	getClient: function(config) {
		return connect(config);
	},
	load: function(config, sql, domain, opts, idColumns) {
		let client = (config.query) ? config : this.getClient(config);
		if (Array.isArray(idColumns)) {
			return sqlLoaderJoin(client, idColumns, sql, domain, opts);
		} else {
			return sqlLoader(client, sql, domain, opts);
		}
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

			client.end(() => {
				console.log('Closing the SQLSERVER connection');
				// end the stream
				stream.end();
			});
		}, {inRowMode: false});

		return stream;
	},
	domainObjectLoader: function(bot_id, dbConfig, sql, domain, opts, callback) {
		if (opts.snapshot) {
			snapShotter(bot_id, connect(dbConfig), dbConfig.table, dbConfig.id, domain, {
				event: opts.outQueue
			}, callback);
		} else {
			let stream = leo.read(bot_id, opts.inQueue, {start: opts.start});
			let stats = ls.stats(bot_id, opts.inQueue);
			let client = this.getClient(dbConfig);

			ls.pipe(stream, stats, this.load(client, sql, domain, opts, dbConfig.id), leo.load(bot_id, opts.outQueue || dbConfig.table), err => {
				client.end(() => {
					if (err) return callback(err);
					return stats.checkpoint(callback);
				});
			});
		}
	},
	checksum: function(config) {
		return checksum(connect(config));
	},
	connect: connect,
  dol: DomainObjectLoader,
  DomainObjectLoader: DomainObjectLoader
};

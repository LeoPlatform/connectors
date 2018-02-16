"use strict";
const connect = require("./lib/connect.js");
const sqlLoader = require("../lib/sql/loader");
const PassThrough = require("stream").PassThrough;

module.exports = {
	load: function(config, sql, domain) {
		return sqlLoader(() => connect(config), sql, domain);
	},
	streamChanges: function(config, tables) {
		let client = connect(config);

		let stream = new PassThrough({
			objectMode: true
		});
		let obj = {
			payload: {}
		};

		/*@TDOD  Gotta store and use this*/
		let version = 1;

		let sqlTables = tables.map(t => {
			obj.payload[t] = [];
			return `SELECT '${t}' as tableName, id
					 FROM  CHANGETABLE(CHANGES test, ${version}) AS CT  `;
		});
		client.query(sqlTables.join(" UNION "), (err, result) => {
			if (!err) {
				result.forEach(r => {
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
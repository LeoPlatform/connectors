require("./tediousAsRow.js");
const mssql = require("mssql");
const logger = require("leo-sdk/lib/logger")("connector.sql.mssql");

// require("leo-sdk/lib/logger").configure(/.*/, {
// 	all: true
// });
module.exports = function(config) {
	const pool = new mssql.ConnectionPool(Object.assign({
		user: 'root',
		password: 'test',
		server: 'localhost',
		database: 'sourcedata',
		port: 1433,
		requestTimeout: 1000 * 50,
		pool: {
			max: 1
		}
	}, config));

	let buffer = [];
	let isConnected = false;

	pool.connect(err => {
		//console.log("Got a connection thing", err, buffer.length)
		isConnected = true;
		if (err) {
			console.log(err);
			process.exit();
		} else if (buffer.length) {
			buffer.forEach(i => {
				client.query(i.query, i.params, (err, result, fields) => {
					i.callback(err, result, fields);
				}, i.opts);
			});
		}
	});

	let queryCount = 0;
	let client = {
		query: function(query, params, callback, opts = {}) {
			if (typeof params == "function") {
				opts = callback;
				callback = params;
				params = {};
			}
			opts = Object.assign({
				inRowMode: false,
				stream: false
			}, opts || {});



			if (!isConnected) {
				console.log("buffering query");
				buffer.push({
					query: query,
					params: params,
					callback: callback,
					opts: opts
				});
			} else {
				let queryId = ++queryCount;
				let log = logger.sub("query");
				let request = pool.request();
				log.info(`SQL query #${queryId} is `, query);
				log.time(`Ran Query #${queryId}`);

				if (params) {
					for (let i in params) {
						request.input(i, params[i]);
					}
				}
				let queryType = "query";
				if (opts.inRowMode) {
					queryType = "queryRow";
				}
				if (opts.stream === true) {
					request.stream = true;
				}
				request[queryType](query, function(err, result) {
					log.timeEnd(`Ran Query #${queryId}`);
					if (err) {
						log.error(`Had error #${queryId}`, query, err);
						if (callback) callback(err);
					} else {
						let columns = result.columns || (result.recordset && Object.keys(result.recordset[0] || {}).map(k => ({
							name: k
						})));
						if (callback) callback(null, result.recordset, columns);
					}
				});

				return request;
			}
		},
		queryRow: function(query, params, callback, opts = {}) {
			if (typeof params == "function") {
				opts = callback;
				callback = params;
				params = {};
			}
			return this.query(query, params, callback, Object.assign(opts, {
				inRowMode: true
			}));
		},
		range: function(table, id, opts, callback) {
			client.query(`select min(${id}) as min, max(${id}) as max, count(${id}) as total from ${table}`, (err, result) => {
				if (err) return callback(err);
				callback(null, {
					min: result[0].min,
					max: result[0].max,
					total: result[0].total
				});
			});
		},
		nibble: function(table, id, start, min, max, limit, reverse, callback) {
			let sql;
			if (reverse) {
				sql = `select ${id} as id from ${table}  
							where ${id} <= ${start} and ${id} >= ${min}
							ORDER BY ${id} desc
							OFFSET ${limit-1} ROWS 
							FETCH NEXT 2 ROWS ONLY`;
			} else {
				sql = `select ${id} as id from ${table}  
							where ${id} >= ${start} and ${id} <= ${max}
							ORDER BY ${id} asc
							OFFSET ${limit-1} ROWS 
							FETCH NEXT 2 ROWS ONLY`;
			}

			client.query(sql, callback);
		},
		getIds: function(table, id, start, end, reverse, callback) {
			if (reverse) {
				sql = `select ${id} as id from ${table}  
					where ${id} <= ${start} and ${id} >= ${end}
					ORDER BY ${id} desc`;
			} else {
				sql = `select ${id} as id from ${table}  
					where ${id} >= ${start} and ${id} <= ${end}
					ORDER BY ${id} asc`;
			}

			client.query(sql, callback);
		},
		end: pool.close
	};
	return client;
};

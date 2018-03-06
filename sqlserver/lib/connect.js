const mssql = require("mssql");
const logger = require("leo-sdk/lib/logger")("connector.sql.mssql");

// require("leo-sdk/lib/logger").configure(/.*/, {
// 	all: true
// });
module.exports = function (config) {
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
				client.query(i.query, i.params, (err, result) => {
					err && console.log(i.query, err);
					i.callback(err, result)
				});
			});
		}
	});

	let queryCount = 0;
	let client = {
		query: function (query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}

			if (!isConnected) {
				console.log("buffering query");
				buffer.push({
					query: query,
					params: params,
					callback: callback
				});
			} else {
				let queryId = ++queryCount;
				let log = logger.sub("query");
				let request = pool.request();
				log.info(`SQL query #${queryId} is `, query);
				log.time(`Ran Query #${queryId}`);

				console.log(params);

				if (params) {
					for (let i in params) {
						request.input(i, params[i]);
					}
				}

				request.query(query, function (err, result, fields) {
					log.timeEnd(`Ran Query #${queryId}`);
					if (err) {
						log.info("Had error", err);
						callback(err);
					} else {
						callback(null, result.recordset, Object.keys(result.recordset[0] || {}).map(k => ({name:k})));
					}
				})
			}
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
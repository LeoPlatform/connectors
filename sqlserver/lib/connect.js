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
				client.query(i.query, (err, result) => {
					err && console.log(i.query, err);
					i.callback(err, result)
				});
			});
		}
	});

	let queryCount = 0;
	let client = {
		query: function (query, callback) {
			if (!isConnected) {
				console.log("buffering query");
				buffer.push({
					query: query,
					callback: callback
				});
			} else {
				let queryId = ++queryCount;
				let log = logger.sub("query");
				log.info(`SQL query #${queryId} is `, query);
				log.time(`Ran Query #${queryId}`);
				pool.request().query(query, function (err, result, fields) {
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
		end: pool.close
	};
	return client;
};
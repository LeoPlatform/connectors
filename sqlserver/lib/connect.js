const mssql = require("mssql");
const logger = require("leo-sdk/lib/logger")("connector.sql.mssql");


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
		if (err) {
			console.log(err);
			process.exit();
		} else if (buffer.length) {
			console.log("connected");
			isConnected = true;
			buffer.forEach(i => {
				client.query(i.query, i.callback);
			});
		}
	});

	let queryCount = 0;
	let client = {
		query: function(query, callback) {
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
				pool.request().query(query, function(err, result, fields) {
					log.timeEnd(`Ran Query #${queryId}`);
					if (err) {
						log.error("Had error", err);
						callback(err);
					} else {
						callback(null, result.recordset, fields);
					}
				})
			}
		},
		end: pool.close
	};
	return client;
};
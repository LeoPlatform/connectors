const {
	Pool,
	Client
} = require('pg')
const logger = require("leo-sdk/lib/logger")("connector.sql.postgres");
// var TIMESTAMP_OID = 1114;

require('pg').types.setTypeParser(1114, (val) => {
	val += "Z";
	console.log(val);
	return moment(val).unix() + "  " + moment(val).utc().format();
});

module.exports = function(config) {
	const pool = new Pool(Object.assign({
		user: 'root',
		host: 'localhost',
		database: 'test',
		password: 'a',
		port: 5432,
	}, config));

	let queryCount = 0;
	return {
		query: function(query, callback) {
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			pool.query(query, function(err, result) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error", err);
					callback(err);
				} else {
					callback(null, result.rows, result.fields);
				}
			})
		},
		end: () => {
			pool.end();
		}
	};
};
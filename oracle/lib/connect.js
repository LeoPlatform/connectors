'use strict';
const oracle = require("oracledb");
const logger = require("leo-logger")("connector.sql.oracle");
let connections = {};

module.exports = function(config) {
	// Support connectionString, connectString, or host/server, port, and database
	let connectionString = '';
	if (config.connectionString) {
		connectionString = config.connectionString;
	} else if (config.connectString) {
		connectionString = config.connectString;
	} else if ((config.host || config.server) && config.database) {
		connectionString = (config.host || config.server) + ':' + (config.port || 1521) + `/${config.database}`;
	} else {
		throw new Error('connectionString or host and database are required');
	}

	config = Object.assign({
		user: 'root',
		password: 'test',
		connectionString: connectionString
	}, config);
	let connectionHash = JSON.stringify(config);
	let pool;
	let isConnected = false;
	let buffer = [];

	if (!(connectionHash in connections)) {
		logger.log("CREATING NEW ORACLE CONNECTION");
		connections[connectionHash] = oracle.createPool(config).then(c => {
			c.getConnection().then(conn => {
				pool = conn;
				isConnected = true;

				if (buffer.length) {
					buffer.forEach(i => {
						client.query(i.query, (err, result, fields) => {
							i.callback(err, result, fields);
						});
					});
				}

			}).catch(err => {
				delete connections[connectionHash];
				console.log(err);
			});
		}).catch(err => {
			delete connections[connectionHash];
			console.log(err);
		});
	} else {
		logger.log("REUSING EXISTING ORACLE CONNECTION");
	}

	let queryCount = 0;

	let client = {
		query: function(query, params, callback, opts = {}) {

			if (typeof params == "function") {
				opts = callback;
				callback = params;
				params = [];
			}

			if (!isConnected) {
				logger.log('buffering query');
				buffer.push({
					query: query,
					callback: callback
				});
			} else {
				/**
				 * @todo
				 * add streaming support (oracledb supports it)
				 * Add inRowMode support to toggle between key=>value pairs or row arrays. (3rd parameter, outFormat: oracle.OBJECT)
				 */
				opts = Object.assign({
					inRowMode: false,
					stream: false,
					lowercase: true
				}, opts || {});

				let queryId = ++queryCount;
				let log = logger.sub("query");
				log.info(`SQL query #${queryId} is `, query.slice(0, 300));
				log.time(`Ran Query #${queryId}`);

				pool.execute(query, {}, {}, function(err, result) {
					log.timeEnd(`Ran Query #${queryId}`);
					if (err) {
						log.error(`Had error #${queryId}`, query, err);
					}

					if (opts.lowercase) {
						result.metaData.map(field => {
							field.name = field.name.toLowerCase();

							return field;
						});
					}

					callback(err, result.rows, result.metaData);
				});
			}
		},
		end: function(callback) {
			connections[connectionHash] = undefined;
			return pool.close(callback);
		},
		disconnect: function(callback) {
			return this.end(callback);
		}
	};
	return client;
};

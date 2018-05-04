"use strict";

const refUtil = require('leo-sdk/lib/reference.js');
const async = require('async');
const MAX = 500;

const moment = require('moment');

module.exports = function(event, context, sdk) {
	if (!event || !context || !sdk) {
		throw new Error('Required parameters for the helperFactor are: ‘event’, ‘context’, ‘leo-sdk’');
	}

	/**
	 * Build domain objects from an array of id's
	 * @param params {
	 *  connection
	 *  connector (sqlserver|mysql|postgres|mongo)
	 *  table
	 *  pk
	 *  query
	 *  snapshot (optional)
	 *  }
	 */
	this.buildDomainObjects = function (params) {
		params.snapshot = params.snapshot || process.env.snapshot;

		// use the event.connection if it exists and we don't specifically pass in a connection
		if (!params.connection && event.connection) {
			params.connection = event.connection;
		}

		// if we're doing a snapshot, put the table and id into the connection object
		if (params.snapshot) {
			params.connection.table = params.table;
			params.connection.id = params.pk;
		}

		let tables = {},
			joins = {},
			sqlQuery;

		return {
			// Table => primary key || SELECT query to get the primary key
			mapDomainId: function(table, pk) {
				tables[table] = pk;

				return this;
			},

			query: function(sql) {
				sqlQuery = sql;

				return this;
			},

			joinOneToMany: function(name, pk, sql) {
				joins[name] = {type: 'one_to_many', name: name, pk: pk, query: sql};

				return this;
			},

			// do stuff to build the domain objects
			run: function (callback) {
				let readParams = {};
				if (params.start) {
					readParams.start = params.start;
				}

				params.connector.domainObjectLoader(event.botId, params.connection, (obj, done) => {
					let objArray = [];

					Object.keys(tables).forEach((table) => {
						// only process if we have any data for this table
						if (obj[table] && obj[table].length) {

							// if the value of any of the tables is a SELECT query, replace ? with the IDs in obj[table]
							if (tables[table].match(/^SELECT/)) {
								async.doWhilst((done) => {

									// split the ID's up into no more than 5k for each query
									let ids = obj[table].splice(0, MAX);
									objArray.push(tables[table].replace(/\?/g, ids.filter((id) => {return id;}).join()));
									done();
								}, () => obj[table].length);
							} else {
								// we just have id's. Push them into the object
								objArray.push(obj[table]);
							}
						}
					});

					done(null, objArray);
				},
				function (ids, builder) {
					let idsList = ids.filter((id) => {return id;}).join();
					let builderSql = builder(params.pk, sqlQuery.replace(/\?/g, idsList));

					// build the joins
					Object.keys(joins).forEach((name) => {
						let join = joins[name];
						if (join.type === 'one_to_many') {
							builderSql.joinOneToMany(join.table, join.pk, join.query.replace(/\?/g, idsList), join.transform);
						} else if (join.type === 'one_to_one') {
							builderSql.join(join.table, join.pk, join.query.replace(/\?/g, idsList), join.transform);
						}
					});

					return builderSql;
				},
				{
					snapshot: params.snapshot,
					inQueue: event.source,
					outQueue: event.destination,
					start: params.start || null
				},
				callback);
			}
		};
	};

	/**
	 * Build Data Warehouse objects from domain objects
	 * @param params {
	 *  ls: leo.streams
	 *  logEvents: number of events to process before outputting a log row
	 *  start: position to start from
	 *  devnull (will send output to devnull - optional)
	 * }
	 * @returns {exports}
	 */
	this.loadDWObjects = function (params) {
		params = {
			ls: params.ls,
			logEvents: params.logEvents || 1000,
			start: params.start || undefined
		};

		if (!params.ls) {
			params.ls = sdk.streams;
		}

		let entities = [];

		return {
			addDimension: function (name, dataTransform) {
				if (typeof dataTransform !== 'function') {
					throw new Error('2nd parameter of addDimension must be a function');
				}

				entities.push({
					"type": "dimension",
					"table": name,
					"data": dataTransform
				});

				return this;
			},
			addFact: function (name, dataTransform) {
				if (typeof dataTransform !== 'function') {
					throw new Error('2nd parameter of addFact must be a function');
				}

				entities.push({
					"type": "fact",
					"table": name,
					"data": dataTransform
				});

				return this;
			},
			run: function (callback) {
				let i = 0,
					stats = params.ls.stats(event.botId, event.source);

				let end;
				if (params.devnull) {
					end = params.ls.devnull();
				} else {
					end = sdk.load(event.botId, event.destination);
				}

				let readParams = {};
				if (params.start) {
					readParams.start = params.start;
				}

				params.ls.pipe(sdk.read(event.botId, event.source, readParams)
					, stats
					// , params.ls.log()
					, params.ls.through(function (obj, done) {
						i++;
						if (i % params.logEvents === 0) {
							console.log(i);
						}

						// process and push the events downstream
						entities.forEach((entityObj) => {
							let data = entityObj.data.call(this, obj.payload);

							if (Array.isArray(data)) {
								data.forEeach((dataObj) => {
									this.push({
										"type": entityObj.type,
										"table": entityObj.table,
										"data": dataObj
									});
								})
							} else if (Object.keys(data).length) {
								this.push({
									"type": entityObj.type,
									"table": entityObj.table,
									"data": data
								});
							}
						});
						done();
					})
					, end
					, (err, data) => {
						err && console.log('Error:', err);
						console.log('Completed. Remaining Time:', context.getRemainingTimeInMillis());
						if (err) {
							callback(err);
						} else {
							stats.checkpoint(callback)
						}
					});
			}
		};
	};

	/**
	 * Filter bin logs to pull changed ids for specific tables
	 * @param params
	 * @todo
	 */
	this.filterBinLogs = function (params) {};

	/**
	 * Create a change stream and get changed ids for specific tables
	 * @param params {
	 *  connector (leo-connector-(dbtype))
	 *  leo (leo-sdk)
	 *  table
	 *  pk (primary key)
	 *  start (optional)
	 *  ls (leo.streams - optional)
	 *  devnull (will send output to devnull - optional)
	 * }
	 */
	this.trackTableChanges = function (params) {
		if (!params.connector) {
			throw new Error('Connector is a required parameter, and must be one of leo-connector-(sqlserver|postgres|mysql|etc...).');
		}

		if (!params.ls) {
			params.ls = sdk.streams;
		}

		let trackedTables = {};

		// get the starting point
		let queue = refUtil.ref(event.source);
		let start = params.start
			|| event.start
			|| (event.__cron
				&& event.__cron.checkpoints
				&& event.__cron.checkpoints.read
				&& ((event.__cron.checkpoints.read[queue] && event.__cron.checkpoints.read[queue].checkpoint)
					|| (event.__cron.checkpoints.read[queue.id] && event.__cron.checkpoints.read[queue.id].checkpoint)))
			|| '0.0';

		return {
			trackTable: function(table, pk) {
				trackedTables[table] = pk;
				return this;
			},
			run: function(callback) {
				let stream = params.connector.streamChanges(params.connection, trackedTables, {
					start: start,
					source: event.source
				});

				let end;
				if (params.devnull) {
					end = params.ls.devnull();
				} else {
					end = sdk.load(event.botId, event.destination);
				}

				params.ls.pipe(stream,
					// ls.log(),
					end, (err) => {
						console.log("all done");
						console.log(err);
						// do work
						callback(err);
					});
			}
		}
	};

	this.checksum = function (params) {
		if (!params.source_connector || !params.slave_connector || !params.checksum_connector) {
			console.log(params);
			throw new Error('Missing one of the following parameter values: source_connector, source_connection, slave_connector, slave_connection, or checksum_connector');
		}
		let sourceParams = [];
		let slaveParams = [];
		let checksum = params.checksum_connector;

		if (!params.ls) {
			params.ls = sdk.streams;
		}

		return {
			sourceQuery: function(query) {
				sourceParams.query = query;

				return this;
			},
			slaveQuery: function(query) {
				slaveParams.query = query;

				return this;
			},
			run: function(callback) {
				let system = event.botId;

				let source = checksum.lambdaConnector(event.botId, "SqlserverConnector", sourceParams);
				let slave = checksum.lambdaConnector(event.botId, "PostgresConnector", slaveParams);

				checksum.checksum(system, system, source, slave, {
					stopOnStreak: 1750000,
					stop_at: moment().add({
						minutes: 4
					}),
					maxLimit: 2500,
					loadSize: 50000,
					limit: 2500,
					reverse: true,
					sample: true,
					version: 2,
					queue: {
						name: "test-checksum.output",
						transform: params.ls.through((obj, done) => {
							done(null, {
								orders: obj.missing.concat(obj.incorrect)
							});
						})
					},
					skipBatch: true,
					showOutput: true
				})
				.then((result) => {
						console.log(result);
					}, (err) => {
						console.log(err);
					});
			}
		}
	};
};

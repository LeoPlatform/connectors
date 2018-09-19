"use strict";

const refUtil = require('leo-sdk/lib/reference.js');
const async = require('async');
const MAX = 5000;
const sqlstring = require('sqlstring');

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

		// put the table and id into the connection object if we haven't already done so
		params.connection.table = params.connection.table || params.table;
		params.connection.id = params.connection.id || params.pk;

		let tables = {},
			joins = {},
			sqlQuery,
			databases = [];

		return {
			// Table => primary key || SELECT query to get the primary key
			mapDomainId: function(table, pk) {
				tables[table] = pk || true;

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

			includeSchema: function(database) {
				if (Array.isArray(database)) {
					database.forEach(d => {
						if (databases.indexOf(d) === -1) {
							databases.push(d);
						}
					});
				} else {
					if (databases.indexOf(database) === -1) {
						databases.push(database);
					}
				}

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

					// @todo handle deletes separately
					if (obj.update) {
						obj = obj.update;
					}

					Object.keys(obj).forEach(database => {
						// if it's an array than it's a table, not a database
						if (Array.isArray(obj[database])) {
							let table = database;

							if (tables[table]) {
								processIds(obj, objArray, table, tables, params);
							}
						} else if (typeof obj[database] === 'object') {
							// then it's actually a database
							if (databases.length && databases.indexOf(database) !== -1 || !databases.length) {
								Object.keys(obj[database]).forEach(table => {
									// process if this is one of the tables we have selected for processing.
									if (tables[table]) {
										processIds(obj[database], objArray, table, tables, params);
									}
								});
							}
						}
					});

					done(null, objArray);
				},
				function (ids, builder) {
					let idsList = ids;
					if (Array.isArray(ids)) {
						idsList = ids.filter(id => {return id != undefined}).map(id => {
							if (isNaN(id)) {
								return sqlstring.escape(id);
							}

							return id;
						}).join();
					}

					let builderSql = builder(params.pk, sqlQuery.replace(/\?/g, idsList));

					// build the joins
					Object.keys(joins).forEach((name) => {
						let join = joins[name];
						builderSql.joinOneToMany(join.name, join.pk, join.query.replace(/\?/g, idsList), join.transform);
					});

					return builderSql;
				},
				{
					snapshot: params.snapshot,
					inQueue: event.source,
					outQueue: event.destination,
					start: params.start || null,
					limit: params.limit || MAX
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
		params = Object.assign({
			ls: sdk.streams,
			logEvents: 1000,
			start: undefined
		}, params || {});

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
							stats.checkpoint(callback);
						}
					});
			}
		};
	};

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
		let omitTables = [];

		// get the starting point
		let queue = refUtil.ref(event.source);
		params.source = event.source;
		params.start = params.start
			|| event.start
			|| (event.__cron
				&& event.__cron.checkpoints
				&& event.__cron.checkpoints.read
				&& ((event.__cron.checkpoints.read[queue] && event.__cron.checkpoints.read[queue].checkpoint)
					|| (event.__cron.checkpoints.read[queue.id] && event.__cron.checkpoints.read[queue.id].checkpoint)))
			|| params.defaultCheckpoint || '0.0';

		return {
			trackTable: function(table, pk) {
				trackedTables[table] = pk;
				return this;
			},
			omitTable: function(table) {
				if (Array.isArray(table)) {
					table.forEach(t => {
						if (omitTables.indexOf(t) === -1) {
							omitTables.push(t);
						}
					});
				} else {
					if (omitTables.indexOf(table) === -1) {
						omitTables.push(table);
					}
				}

				return this;
			},
			run: function(callback) {
				params.omitTables = omitTables;
				let stream = params.connector.streamChanges(params.connection, trackedTables, params);

				let end;
				if (params.devnull) {
					end = params.ls.devnull();
				} else {
					end = sdk.load(event.botId, event.destination);
				}

				params.ls.pipe(stream,
					end, (err) => {
						console.log("all done");
						console.log(err);
						// do work
						callback(err);
					});
			}
		};
	};

	// add an alias so this makes more sense
	this.trackDatabaseChanges = this.trackTableChanges;
};

function processIds(obj, objArray, table, tables, params)
{
	// only process if we have any data for this table
	if (obj[table] && obj[table].length) {
		if (Array.isArray(tables[table])) { // if we passed in an array of primary keys
			// turn the object into an array of items
			objArray.push(obj[table].map(row => {
				let array = [];
				for (let key of tables[table]) {
					array.push(row[key]);
				}

				return array;
			}));

			// if the value of any of the tables is a SELECT query, replace ? with the IDs in obj[table]
		} else if (typeof tables[table] === 'string' && tables[table].match(/^SELECT/)) {
			async.doWhilst((done) => {

				// split the ID's up into no more than 5k for each query
				let ids = obj[table].splice(0, params.limit || MAX);
				objArray.push(tables[table].replace(/\?/g, ids.filter(id => {
					return id != undefined
				}).join()));
				done();
			}, () => obj[table].length);
		} else {
			// we just have id's. Push them into the object
			objArray.push(obj[table]);
		}
	}
}
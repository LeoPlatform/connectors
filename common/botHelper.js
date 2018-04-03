"use strict";

const refUtil = require('leo-sdk/lib/reference.js');
const loaderBuilder = require('./sql/loaderBuilder');

module.exports = function(event, context, callback, sdk) {
	if (!sdk) {
		throw new Error('leo-sdk must be passed into the helperFactory');
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
		params = {
			connection: params.connection,
			connector: params.connector,
			table: params.table,
			pk: params.pk,
			query: params.query,
			snapshot: params.snapshot || process.env.snapshot
		};

		params.connection.table = params.table;
		params.connection.id = params.pk;

		params.connector.domainObjectLoader(event.botId, params.connection, (obj, done) => {
			done(null, [
				obj[params.table]
			]);
		}, function (ids, builder) {
			let idsList = ids.join();
			let builderSql = builder(params.id, params.query(idsList));

			if (params.joins) {
				params.join.forEach((join) => {
					if (join.type === 'one_to_many') {
						builderSql.joinOneToMany(join.table, join.pk, join.query(idsList), join.transform);
					} else if (join.type === 'one_to_one') {
						builderSql.join(join.table, join.pk, join.query(idsList), join.transform);
					}
				});
			}
			return builderSql;
		}, {
			snapshot: params.snapshot,
			inQueue: event.source,
			outQueue: event.destination
		}, callback);
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
					"entity": name,
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
					"entity": name,
					"data": dataTransform
				});

				return this;
			},
			run: function () {
				let i = 0,
					stats = params.ls.stats(event.botId, event.source),
					logEvents = params.logEvents || 1000;

				let end;
				if (params.devnull) {
					end = params.ls.devnull();
				} else {
					end = sdk.load(event.botId, event.destination);
				}

				params.ls.pipe(sdk.read(event.botId, event.source, {start: params.start})
					, stats
					// , params.ls.log()
					, params.ls.through(function (obj, done) {
						i++;
						if (i % logEvents === 0) {
							console.log(i);
						}

						// process and push the events downstream
						entities.forEach((entityObj) => {
							let data = entityObj.data.call(this, obj.payload);

							this.push({
								"type": entityObj.type,
								"entity": entityObj.entity,
								"data": data
							});
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
	 */
	this.filterBinLogs = function (params) {

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
		let trackedTables = loaderBuilder.createChangeTrackingObject();

		if (!params.connector) {
			throw new Error('Connector is a required parameter, and must be one of leo-connector-(sqlserver|postgres|mysql|etc...).');
		}

		if (params.table && params.pk) {
			trackedTables.trackTable(params.table, params.pk);
		}

		if (!params.ls) {
			params.ls = sdk.streams;
		}

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
			trackTable: function(table, pk, query) {
				trackedTables.trackTable(table, pk);

				if (query) {
					trackedTables.mapToDomainId(table, query);
				}
				return this;
			},
			run: function() {
				let stream = params.connector.streamChanges(params.connection, trackedTables.getTrackedTables(), {
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
	}
};
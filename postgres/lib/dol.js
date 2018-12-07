const leo = require("leo-sdk");
const ls = leo.streams;
const async = require("async");
const logger = require('leo-logger');
const parent = require('leo-connector-common/dol');
const format = require('pg-format');

module.exports = class Dol extends parent {
	constructor(client) {
		super(client);
	}

	buildDomainObject(domainObject, ids, push, callback) {
		let opts = {};
		let sqlClient = this.client;
		let joinsCount = Object.keys(domainObject.joins).length;

		async.eachLimit(Object.entries(ids), 5, ([schema, ids], callback) => {
			let tasks = [];
			let domains = {};
			let queryIds = [];
			logger.log(`Processing ${ids.length} from ${schema}`);
			ids.forEach(id => {

				// if the id is an object, it's a composite key
				if (typeof id === 'object') {
					// change the id to be a string with keys separated by a dash
					queryIds.push(Object.values(id));
					id = Object.values(id).join('-');
				} else {
					queryIds.push(id);
				}

				domains[id] = {};
				Object.keys(domainObject.joins).forEach(name => {
					domains[id][name] = [];
				});
			});


			tasks.push(done => {
				if (!domainObject.domainIdColumn) {
					logger.error('[FATAL ERROR]: No ID specified');
				}

				let query = domainObject.sql.call({
					database: schema,
					schema: schema,
					client: sqlClient
				}, {
					ids: queryIds,
					database: schema,
					schema: schema,
					client: sqlClient
				});

				sqlClient.query(this.getFormattedQuery(queryIds, query), (err, results, fields) => {
					if (err) return done(err);

					this.mapResults(results, fields, row => {
						let domainId = row[domainObject.domainIdColumn];
						if (domainObject.transform) {
							row = domainObject.transform(row);
						}
						delete row._domain_id;

						if (!domainId) {
							logger.error('ID: "' + domainObject.domainIdColumn + '" not found in object:');
						} else if (!domains[domainId]) {
							logger.error('ID: "' + domainObject.domainIdColumn + '" with a value of: "' + domainId + '" does not match any ID in the domain object. This could be caused by using a WHERE clause on an ID that differs from the SELECT ID');
						} else {
							//We need to keep the domain relationships in tact
							domains[domainId] = Object.assign(domains[domainId], row);
						}
					});
					done();
				}, {
					inRowMode: true
				});
			});

			Object.keys(domainObject.joins).forEach(name => {
				let t = domainObject.joins[name];
				t.domainIdColumn = t.domainIdColumn || "_domain_id";
				let query = t.sql.call({
					database: schema,
					schema: schema,
					client: sqlClient
				}, {
					ids: queryIds,
					database: schema,
					schema: schema,
					client: sqlClient
				});
				tasks.push(done => {
					sqlClient.query(this.getFormattedQuery(queryIds, query), (err, results, fields) => {
						if (err) {
							return done(err);
						} else if (!results.length) {
							return done();
						}

						this.mapResults(results, fields, row => {
							let domainId = row[t.domainIdColumn];
							delete row._domain_id;

							if (t.transform) {
								row = t.transform(row);
							}

							domains[domainId][name].push(row);
						});
						done();
					}, {
						inRowMode: true
					});
				});
			});
			let limit = 5;
			async.parallelLimit(tasks, limit, (err) => {
				if (err) {
					callback(err);
				} else {
					ids.forEach(id => {
						// if the id is an object, it's a composite key
						if (typeof id === 'object') {
							// change the id to be a string with keys separated by a dash
							id = Object.values(id).join('-');
						}

						// skip the domain if there is no data with it
						let keyCount = Object.keys(domains[id]).length;
						if (keyCount === 0) {
							logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
							return;
						} else if (keyCount <= joinsCount) {
							let valid = Object.keys(domainObject.joins).some(k => {
								return Object.keys(domains[id][k] || []).length > 0;
							});
							if (!valid) {
								logger.log('[INFO] Skipping domain id due to empty object. #: ' + id);
								return;
							}
						}
						// let eids = {}; // TODO: get event info
						// let eid = getEid(id, domains[id], eids);
						let event = {
							event: opts.queue,
							id: opts.id,
							payload: domains[id],
							schema: schema,
							domain_id: id
						};

						// remove the schema if it's a dummy we added in translate ids
						if (event.schema === '__database__') {
							delete event.schema;
						}

						push(event);
					});
					callback();
				}
			});
		}, (err) => {
			callback(err);
		});
	}

	getFormattedQuery(ids, query) {
		ids = '(' + ids.join('),(') + ')';

		query = format(query, ids);
		logger.debug('query', query);

		return query;
	}
}

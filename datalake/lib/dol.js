'use strict';

const logger = require('leo-logger');
const parent = require('leo-connector-common/dol');
const sqlstring = require('sqlstring');

// Databricks dialect Domain Object Layer — port of connectors/postgres/lib/dol.js.
// Databricks uses backtick identifier quoting and positional ? binding (not $1).
module.exports = class Dol extends parent {
	constructor(client) {
		super(client);
	}

	buildDomainQuery(domainObject, domains, query, queryIds, done) {
		query = sqlstring.format(query, queryIds);
		logger.debug('Formatted Domain Query', query);

		this.client.query(query, (err, results, fields) => {
			this.processDomainQuery(domainObject, domains, done, err, results, fields);
		}, {
			inRowMode: true
		});
	}

	buildJoinQuery(joinObject, name, domains, query, queryIds, done) {
		query = sqlstring.format(query, queryIds);
		logger.debug('Formatted Join Query', query);

		this.client.query(query, (err, results, fields) => {
			this.processJoinQuery(joinObject, name, domains, done, err, results, fields);
		}, {
			inRowMode: true
		});
	}

	handleTranslateObject(translation) {
		let self = this;
		let queryFn = this.queryToFunction(translation.translation, ['data']);
		return function(data, done) {
			let query = queryFn.call(this, data);
			let ids = data.ids;

			if (translation.keys) {
				ids = data.ids.map(ids => {
					let returnObj = [];
					translation.keys.forEach(key => {
						returnObj.push(ids[key]);
					});
					return returnObj;
				});
			}

			query = sqlstring.format(query, [ids]);
			this.client.query(query, (err, rows) => {
				self.processResults(err, rows, done);
			}, {
				inRowMode: false
			});
		};
	}

	handleTranslateString(translation) {
		let self = this;
		let queryFn = this.queryToFunction(translation, ['data']);
		return function(data, done) {
			let query = queryFn.call(this, data);

			query = sqlstring.format(query, [data.ids]);
			this.client.query(query, (err, rows) => {
				self.processResults(err, rows, done);
			}, {
				inRowMode: false
			});
		};
	}
};

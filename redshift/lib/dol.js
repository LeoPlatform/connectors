'use strict';

const logger = require('leo-logger');
const parent = require('leo-connector-common/dol');
const sqlstring = require('sqlstring');

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
}

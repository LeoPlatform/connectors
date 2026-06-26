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

		// inRowMode:true — leo-connector-common's mapResults (common/dol.js:492) requires
		// positional array rows so it can slice sections demarcated by `prefix_` sentinel
		// columns. connect.js converts Databricks object rows to arrays to satisfy this.
		this.client.query(query, (err, results, fields) => {
			this.processDomainQuery(domainObject, domains, done, err, results, fields);
		}, {
			inRowMode: true
		});
	}

	// connect.js derives `fields` from Object.keys(rows[0]), so an empty result set
	// produces fields=[] — which causes mapResults (common/dol.js:512) to crash on
	// `last.end = fields.length` when last is still null.  processJoinQuery already
	// short-circuits for empty results; this override adds the same guard here.
	processDomainQuery(domainObject, domains, done, err, results, fields) {
		if (err) return done(err);
		if (!results || !results.length) return done();
		return super.processDomainQuery(domainObject, domains, done, err, results, fields);
	}

	buildJoinQuery(joinObject, name, domains, query, queryIds, done) {
		query = sqlstring.format(query, queryIds);
		logger.debug('Formatted Join Query', query);

		// inRowMode:true — same reason as buildDomainQuery above.
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

'use strict';

const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

// ─── Real parent, only AWS-coupling stubbed ───────────────────────────────────
//
// Load the actual leo-connector-common/dol.js source so that mapResults,
// processDomainQuery, processJoinQuery, processResults, and queryToFunction
// are the real implementations.  A hand-rolled ParentStub would only prove
// consistency between the stub and our assumptions — it would mask exactly
// the kind of inRowMode / prefix_-sentinel contract bugs we're guarding here.
//
// leo-sdk and leo-logger are stubbed to avoid AWS initialisation at test time.
// `async` is left real (it's pure JS, always available).
const commonDolPath = require.resolve('leo-connector-common/dol');
const RealParent = proxyquire(commonDolPath, {
	'leo-sdk': {
		streams: {
			pipeline: sinon.stub(),
			through:  sinon.stub(),
			batch:    sinon.stub(),
		},
	},
	'leo-logger': () => ({ debug: () => {}, info: () => {}, error: () => {}, log: () => {}, warn: () => {} }),
});

// Dol under test — inject the real parent, silence the debug logger.
const Dol = proxyquire('../../lib/dol.js', {
	'leo-connector-common/dol': RealParent,
	'leo-logger': () => ({ debug: () => {}, info: () => {}, error: () => {}, log: () => {}, warn: () => {} }),
});

// ─── Shared fixtures ──────────────────────────────────────────────────────────

function makeClient(queryImpl) {
	return { query: sinon.stub().callsFake(queryImpl || (() => {})) };
}

const simpleDomainObj = {
	domainIdColumn: '_domain_id',
	transform: row => row,
};

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('dol.js — Databricks DOL', () => {

	// ── buildDomainQuery ────────────────────────────────────────────────────────

	describe('buildDomainQuery', () => {
		it('bakes queryIds into the SQL via sqlstring — no ? placeholder remains', (done) => {
			// Core Databricks adaptation: Databricks does not support $1 positional
			// bind params, so queryIds are formatted into the SQL string before the
			// query is issued.
			const client = makeClient(); // stub never fires callback
			const dol = new Dol(client);

			dol.buildDomainQuery(
				simpleDomainObj, {},
				'SELECT * FROM d_account WHERE account_id IN (?)',
				[['R1', 'R2']],
				() => {}
			);

			const sql = client.query.firstCall.args[0];
			expect(sql).to.include('R1');
			expect(sql).to.include('R2');
			expect(sql).to.not.include('?');
			done();
		});

		it('calls client.query in 3-arg form: (formattedSql, callback, opts) — params not passed separately', (done) => {
			// Contrast with the leo-connector-common base which calls
			// client.query(query, queryIds, callback, opts) — 4-arg form.
			// The datalake override drops the params arg because they are already baked in.
			const client = makeClient();
			const dol = new Dol(client);

			dol.buildDomainQuery(simpleDomainObj, {}, 'SELECT 1', [], () => {});

			const args = client.query.firstCall.args;
			expect(args[0]).to.be.a('string');
			expect(args[1], 'second arg should be the callback, not a params array').to.be.a('function');
			expect(args[2]).to.deep.equal({ inRowMode: true });
			done();
		});

		it('passes inRowMode:true so the real mapResults receives positional arrays', (done) => {
			// mapResults (leo-connector-common/dol.js:492) calls r.slice() on each row.
			// That only works when rows are arrays — i.e. when inRowMode:true is set.
			// If inRowMode were false (object rows), mapResults would throw.
			const client = makeClient();
			const dol = new Dol(client);

			dol.buildDomainQuery(simpleDomainObj, {}, 'SELECT 1', [], () => {});

			expect(client.query.firstCall.args[2]).to.deep.equal({ inRowMode: true });
			done();
		});

		it('propagates a query error to done', (done) => {
			const queryErr = new Error('DB connection lost');
			const client = makeClient((_sql, cb) => cb(queryErr));
			const dol = new Dol(client);

			dol.buildDomainQuery(simpleDomainObj, {}, 'SELECT 1', [], (err) => {
				expect(err).to.equal(queryErr);
				done();
			});
		});

		it('calls done() without error when the result set is empty', (done) => {
			const client = makeClient((_sql, cb) => cb(null, [], []));
			const dol = new Dol(client);

			dol.buildDomainQuery(simpleDomainObj, {}, 'SELECT 1', [], (err) => {
				expect(err).to.be.undefined;
				done();
			});
		});

		it('hydrates domains via real mapResults — prefix_ sentinel columns produce nested objects', (done) => {
			// This drives the full processDomainQuery → mapResults path through the
			// real leo-connector-common source.  The README domain-object example uses
			// this exact pattern: a JOIN produces flat columns behind a prefix_ sentinel,
			// and mapResults lifts them into a nested object on the domain record.
			//
			// Fields from a query like:
			//   SELECT _domain_id, account_id, name, '' AS prefix_Addr, city, state
			//   FROM d_account JOIN address USING (account_id)
			//   WHERE _domain_id IN (?)
			const fields = [
				{ name: '_domain_id' },
				{ name: 'account_id' },
				{ name: 'name' },
				{ name: 'prefix_Addr' }, // sentinel — everything after → nested under 'Addr'
				{ name: 'city' },
				{ name: 'state' },
			];
			// Row-mode arrays: connect.js converts object rows to arrays when inRowMode:true
			const results = [
				[1, 100, 'Acme Corp',   null, 'Denver', 'CO'],
				[2, 200, 'Widgets Inc', null, 'Austin', 'TX'],
			];

			const client = makeClient((_sql, cb) => cb(null, results, fields));
			const dol = new Dol(client);
			const domains = { 1: {}, 2: {} };

			dol.buildDomainQuery(
				simpleDomainObj, domains,
				'SELECT _domain_id, account_id, name, prefix_Addr, city, state FROM d_account WHERE _domain_id IN (?)',
				[[1, 2]],
				(err) => {
					expect(err).to.not.exist;

					// Top-level fields merged onto the domain entry
					expect(domains[1].account_id).to.equal(100);
					expect(domains[1].name).to.equal('Acme Corp');
					expect(domains[2].account_id).to.equal(200);
					expect(domains[2].name).to.equal('Widgets Inc');

					// prefix_ sentinel produces a nested sub-object
					expect(domains[1].Addr).to.deep.equal({ city: 'Denver', state: 'CO' });
					expect(domains[2].Addr).to.deep.equal({ city: 'Austin', state: 'TX' });

					// _domain_id is stripped from the merged payload (processDomainQuery deletes it)
					expect(domains[1]).to.not.have.property('_domain_id');
					expect(domains[2]).to.not.have.property('_domain_id');

					done();
				}
			);
		});
	});

	// ── buildJoinQuery ──────────────────────────────────────────────────────────

	describe('buildJoinQuery', () => {
		it('bakes queryIds into the SQL via sqlstring — no ? placeholder remains', (done) => {
			const client = makeClient();
			const dol = new Dol(client);

			dol.buildJoinQuery(
				simpleDomainObj, 'line_items', {},
				'SELECT * FROM f_order_item WHERE order_id IN (?)',
				[['OID-1', 'OID-2']],
				() => {}
			);

			const sql = client.query.firstCall.args[0];
			expect(sql).to.include('OID-1');
			expect(sql).to.include('OID-2');
			expect(sql).to.not.include('?');
			done();
		});

		it('calls client.query in 3-arg form: (formattedSql, callback, opts)', (done) => {
			const client = makeClient();
			const dol = new Dol(client);

			dol.buildJoinQuery(simpleDomainObj, 'items', {}, 'SELECT 1', [], () => {});

			const args = client.query.firstCall.args;
			expect(args[1]).to.be.a('function');
			expect(args[2]).to.deep.equal({ inRowMode: true });
			done();
		});

		it('passes inRowMode:true to client.query', (done) => {
			const client = makeClient();
			const dol = new Dol(client);

			dol.buildJoinQuery(simpleDomainObj, 'items', {}, 'SELECT 1', [], () => {});

			expect(client.query.firstCall.args[2]).to.deep.equal({ inRowMode: true });
			done();
		});

		it('propagates a query error to done', (done) => {
			const joinErr = new Error('Timeout');
			const client = makeClient((_sql, cb) => cb(joinErr));
			const dol = new Dol(client);

			dol.buildJoinQuery(simpleDomainObj, 'items', {}, 'SELECT 1', [], (err) => {
				expect(err).to.equal(joinErr);
				done();
			});
		});

		it('calls done() without error when there are no matching join rows', (done) => {
			const client = makeClient((_sql, cb) => cb(null, [], []));
			const dol = new Dol(client);

			dol.buildJoinQuery(simpleDomainObj, 'items', {}, 'SELECT 1', [], (err) => {
				expect(err).to.be.undefined;
				done();
			});
		});

		it('appends join rows to domains[id][name] via real processJoinQuery', (done) => {
			// Drives the full processJoinQuery → mapResults path with real array-mode rows.
			// Each result row is matched to its domain entry by _domain_id and pushed
			// onto domains[id][joinName] — the standard hasMany() pattern.
			const fields = [
				{ name: '_domain_id' },
				{ name: 'item_id' },
				{ name: 'qty' },
			];
			const results = [
				[1, 'ITM-001', 5],
				[1, 'ITM-002', 3],
			];

			const client = makeClient((_sql, cb) => cb(null, results, fields));
			const dol = new Dol(client);
			// processJoinQuery pushes onto the pre-existing array
			const domains = { 1: { items: [] } };

			dol.buildJoinQuery(
				simpleDomainObj, 'items', domains,
				'SELECT _domain_id, item_id, qty FROM f_order_item WHERE _domain_id IN (?)',
				[[1]],
				(err) => {
					expect(err).to.not.exist;
					expect(domains[1].items).to.have.length(2);
					expect(domains[1].items[0]).to.deep.include({ item_id: 'ITM-001', qty: 5 });
					expect(domains[1].items[1]).to.deep.include({ item_id: 'ITM-002', qty: 3 });
					// _domain_id is stripped by processJoinQuery
					expect(domains[1].items[0]).to.not.have.property('_domain_id');
					done();
				}
			);
		});
	});

	// ── handleTranslateObject ───────────────────────────────────────────────────

	describe('handleTranslateObject', () => {
		it('returns a function', () => {
			const dol = new Dol(makeClient());
			const translation = {
				translation: 'SELECT supplier_id FROM t WHERE supplier_id IN (?)',
				keys: ['supplier_id'],
			};
			expect(dol.handleTranslateObject(translation)).to.be.a('function');
		});

		it('projects translation.keys values as an ordered array — not as a keyed object', (done) => {
			// The datalake dol (matching the postgres sibling) builds returnObj = []
			// when extracting keys — producing positional arrays that sqlstring formats
			// as (v1, v2).  The leo-connector-common base builds returnObj = {} (a keyed
			// object), which sqlstring would format differently.  This discriminates the
			// datalake override from the base.
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);

			const translation = {
				translation: 'SELECT id FROM t WHERE (retailer_id, supplier_id) IN (?)',
				keys: ['retailer_id', 'supplier_id'],
			};
			const translateFn = dol.handleTranslateObject(translation);

			translateFn.call(dol, {
				ids: [
					{ retailer_id: 'R1', supplier_id: 'S1', ignored: 'x' },
					{ retailer_id: 'R2', supplier_id: 'S2', ignored: 'y' },
				],
			}, () => {
				const sql = client.query.firstCall.args[0];
				expect(sql).to.include('R1');
				expect(sql).to.include('S1');
				expect(sql).to.include('R2');
				expect(sql).to.not.include('ignored');
				expect(sql).to.not.include("'x'");
				expect(sql).to.not.include('?');
				done();
			});
		});

		it('uses data.ids directly when translation.keys is absent', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({
				translation: 'SELECT id FROM t WHERE id IN (?)',
			});

			translateFn.call(dol, { ids: ['id-A', 'id-B'] }, () => {
				const sql = client.query.firstCall.args[0];
				expect(sql).to.include('id-A');
				expect(sql).to.include('id-B');
				expect(sql).to.not.include('?');
				done();
			});
		});

		it('calls client.query with inRowMode:false', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({ translation: 'SELECT 1' });

			translateFn.call(dol, { ids: ['x'] }, () => {
				expect(client.query.firstCall.args[2]).to.deep.equal({ inRowMode: false });
				done();
			});
		});

		it('calls client.query in 3-arg form (callback as second arg, no separate params)', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({ translation: 'SELECT 1' });

			translateFn.call(dol, { ids: ['a'] }, () => {
				const args = client.query.firstCall.args;
				expect(args[1]).to.be.a('function');
				expect(args[2]).to.deep.equal({ inRowMode: false });
				done();
			});
		});

		it('flattens single-column result rows to a plain value array via processResults', (done) => {
			// processResults: when rows have exactly one key, returns rows.map(firstValue)
			// This is the common shape: SELECT supplier_id FROM t WHERE retailer_id IN (?)
			const client = makeClient((_sql, cb) => cb(null, [{ id: 10 }, { id: 20 }]));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({
				translation: 'SELECT id FROM t WHERE id IN (?)',
			});

			translateFn.call(dol, { ids: ['a'] }, (err, result) => {
				expect(err).to.not.exist;
				expect(result).to.deep.equal([10, 20]);
				done();
			});
		});

		it('returns full row objects when rows have more than one column', (done) => {
			// processResults: when rows have >1 key, returns the rows unchanged
			const rows = [{ retailer_id: 'R1', supplier_id: 'S1' }];
			const client = makeClient((_sql, cb) => cb(null, rows));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({
				translation: 'SELECT retailer_id, supplier_id FROM t WHERE retailer_id IN (?)',
			});

			translateFn.call(dol, { ids: ['R1'] }, (err, result) => {
				expect(err).to.not.exist;
				expect(result).to.deep.equal(rows);
				done();
			});
		});

		it('propagates a query error to done', (done) => {
			const queryErr = new Error('permission denied');
			const client = makeClient((_sql, cb) => cb(queryErr));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateObject({ translation: 'SELECT 1' });

			translateFn.call(dol, { ids: ['x'] }, (err) => {
				expect(err).to.equal(queryErr);
				done();
			});
		});
	});

	// ── handleTranslateString ───────────────────────────────────────────────────

	describe('handleTranslateString', () => {
		it('returns a function', () => {
			const dol = new Dol(makeClient());
			expect(dol.handleTranslateString('SELECT id FROM t WHERE id IN (?)')).to.be.a('function');
		});

		it('formats data.ids into the SQL via sqlstring — no ? placeholder remains', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateString('SELECT id FROM t WHERE id IN (?)');

			translateFn.call(dol, { ids: ['id-X', 'id-Y'] }, () => {
				const sql = client.query.firstCall.args[0];
				expect(sql).to.include('id-X');
				expect(sql).to.include('id-Y');
				expect(sql).to.not.include('?');
				done();
			});
		});

		it('calls client.query with inRowMode:false', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateString('SELECT id FROM t WHERE id IN (?)');

			translateFn.call(dol, { ids: ['a'] }, () => {
				expect(client.query.firstCall.args[2]).to.deep.equal({ inRowMode: false });
				done();
			});
		});

		it('calls client.query in 3-arg form (callback as second arg)', (done) => {
			const client = makeClient((_sql, cb) => cb(null, []));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateString('SELECT 1');

			translateFn.call(dol, { ids: [] }, () => {
				const args = client.query.firstCall.args;
				expect(args[1]).to.be.a('function');
				expect(args[2]).to.deep.equal({ inRowMode: false });
				done();
			});
		});

		it('flattens single-column result rows to a plain value array via processResults', (done) => {
			const client = makeClient((_sql, cb) => cb(null, [
				{ supplier_id: 'SP1' },
				{ supplier_id: 'SP2' },
			]));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateString(
				'SELECT supplier_id FROM suppliers WHERE retailer_id IN (?)'
			);

			translateFn.call(dol, { ids: ['R1'] }, (err, result) => {
				expect(err).to.not.exist;
				expect(result).to.deep.equal(['SP1', 'SP2']);
				done();
			});
		});

		it('propagates a query error to done', (done) => {
			const queryErr = new Error('timeout');
			const client = makeClient((_sql, cb) => cb(queryErr));
			const dol = new Dol(client);
			const translateFn = dol.handleTranslateString('SELECT 1');

			translateFn.call(dol, { ids: ['a'] }, (err) => {
				expect(err).to.equal(queryErr);
				done();
			});
		});
	});
});

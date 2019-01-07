const proxyquire = require('proxyquire').noCallThru();
const { expect } = require('chai');
const es = require('event-stream');
const stream = require('stream');
const { streams: ls } = require("leo-sdk");
const mockConnection = {
	query: (sql, callback) => {
		console.log('CONNECTION QUERY sql: ', sql);
		callback(null, []);
	},
	release: () => {}
};
const escapeValueNoToLower = function(value) {
	if (value.replace) {
		return "'" + value.replace("'", "\\'") + "'";
	} else {
		return value;
	}
};
function mockDwconnect() {
	return {
		query: function(sql, callback) {
			if (!this.queries){
				this.queries = [];
			} 
			console.log('DWCLIENT QUERY sql: ', sql);
			this.queries.push(sql.replace(/\s/g, ""));
			callback(null, []);
		},
		connect: function() {
			return Promise.resolve(mockConnection);
		},
		streamToTable: function(table) {
			console.log('DWCLIENT STREAM TO TABLE table: ', table);
			return ls.devnull();
		},
		describeTable: function(table, callback) {
			console.log('DWCLIENT DESCRIBE TABLE table: ', table);
			callback(null, []);
		},
		getSchemaCache: function() {
			console.log('DWCLIENT GET SCHEMA CACHE');
			return {
				['public.dim_foo']: [
					{ column_name: 'foo_pk1'},
					{ column_name: 'foo_pk2'}
				]
			};
		},
		escapeValueNoToLower,
		auditdate: "'2019-01-07T23:01:32Z'"
	};
}
const dwconnect = proxyquire('../../../lib/dwconnect', {
	'./connect.js': mockDwconnect
});

describe('Warehouse Connector', () => {
	describe('Expiration', () => {
		it("updates composite keys", (done) => {
			const mockDataStream = new stream.Readable({objectMode: true}).wrap(es.readArray([
				{
					"__leo_delete__": [
						"foo_pk1",
						"foo_pk2"
					],
					"__leo_delete_id__": {
						"foo_pk1": "100",
						"foo_pk2": "200"
					},
					"foo_pk1": "100",
					"foo_pk2": "200"
				},
				{
					"__leo_delete__": [
						"foo_pk1",
						"foo_pk2"
					],
					"__leo_delete_id__": {
						"foo_pk1": "101",
						"foo_pk2": "201"
					},
					"foo_pk1": "101",
					"foo_pk2": "201"
				}
			]));
			const table = 'dim_foo';
			const sk = 'foo_key';
			const nk = ['foo_pk1', 'foo_pk2'];
			const scds = { 0: [], 1: [], 2: [], 6: [] };
			const dwclient = dwconnect({ host: 'fake' });
			dwclient.importDimension(mockDataStream, table, sk, nk, scds, (err) => {
				if (err) return done(err);

				expect(dwclient.queries).to.include(`
				update public.dim_foo set _enddate = '2019-01-07T23:01:32Z'
				where (foo_pk1, foo_pk2)
				in (('100','200'),('101','201'))and _current = true
				`.replace(/\s/g, ""));

				done();
			});
		});
	});
});


const { assert, expect } = require('chai');
const { streams: ls } = require('leo-sdk');
const load = require('../../datawarehouse/load');
const es = require('event-stream');

const tableConfig = {
	"dim_foo_nk_single": {
		"structure": {
			"foo_key": "sk",
			"id": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo_nk_single",
		"label": "Foo Label",
		"isDimension": true
	},	
	"dim_foo_nk_named": {
		"structure": {
			"foo_key": "sk",
			"foo_id": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo_nk_named",
		"label": "Foo Label",
		"isDimension": true
	},	
	"dim_foo": {
		"structure": {
			"foo_key": "sk",
			"foo_pk1": {
				"type": "text",
				"nk": true
			},
			"foo_pk2": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo",
		"label": "Foo Label",
		"isDimension": true
	},
	"fact_bar": {
		"structure": {
			"bar_id": {
				"type": "text",
				"nk": true
			},
			"foo_pk1": {
				"type": "text",
				"dimension": "dim_foo",
				"on": {
					"foo_pk1": "foo_pk1",
					"foo_pk2": "foo_pk2"
				},
				"dim_column": "foo_key"
			},
			"foo_pk2": "text",
		},
		"identifier": "fact_bar",
		"label": "Bar Label"
	}
};

const client = {
	insertMissingDimensions: function (usedTables, tableConfig, tableSks, tableNks, callback) {
		console.log("INSERT MISSING DIMENSIONS usedTables", usedTables);
		console.log("INSERT MISSING DIMENSIONS tableConfig", tableConfig);
		console.log("INSERT MISSING DIMENSIONS tableSks", tableSks);
		console.log("INSERT MISSING DIMENSIONS tableNks", tableNks);
		callback();
	},
	importDimension: function (stream, table, sk, nk, scds, callback) {
		console.log("IMPORT DIMENSIONS table: ", table);
		console.log("IMPORT DIMENSIONS sk: ", sk);
		console.log("IMPORT DIMENSIONS nk: ", nk);
		console.log("IMPORT DIMENSIONS scds: ", scds);
		this.importDimension.streamedValues = [];
		ls.pipe(stream, ls.log("TO IMPORT DIM"), ls.through((obj, done) => {
			this.importDimension.streamedValues.push(obj);
			done(null, obj);
		}),ls.devnull(), callback);
	},
	importFact: function (stream, table, ids, callback) {
		console.log("IMPORT FACT table: ", table);
		console.log("IMPORT FACT ids: ", ids);
		this.importFact.streamedValues = [];
		ls.pipe(stream, ls.log("TO IMPORT FACT"), ls.through((obj, done) => {
			this.importFact.streamedValues.push(obj);
			done(null, obj);
		}), ls.devnull(), callback);
	},
	getDimensionColumn: function() {
		console.log("GET DIM COLUMN");
	}, 
	linkDimensions: function (table, links, nk, callback) {
		console.log("LINK DIMENSIONS table: ", table);
		console.log("LINK DIMENSIONS links: ", links);
		console.log("LINK DIMENSIONS nk: ", nk);
		callback();
	}
};

describe("Data Warehouse", () => {
	describe("load", () => {
		it('delete with named primary key', (done) => {
			const stream = es.readArray([{
				id: 'fooDeleteEventID_A',
				payload: {
					type: "delete",
					data: {
						in: [2001, 2005],
						entities: [
							{
								type: "dimension",
								table: "dim_foo_nk_named", 
								field: "foo_id"
							}
						]
					}
				}
			},{
				id: 'fooDeleteEventID_B',
				payload: {
					type: "delete",
					data: {
						in: [2001, 2004],
						entities: [
							{
								type: "dimension",
								table: "dim_foo_nk_named", 
								field: "foo_id"
							}
						]
					}
				}
			}]);

			load(client, tableConfig, stream, (err) => {
				if (err) {
					done(err);
				} else {
					assert.equal(client.importDimension.streamedValues[0].foo_id, 2004);
					assert.equal(client.importDimension.streamedValues[0].id, '_del_2004');
					assert.equal(client.importDimension.streamedValues[0].__leo_delete__, 'foo_id');
					assert.equal(client.importDimension.streamedValues[0].__leo_delete_id__, 2004);
					assert.equal(client.importDimension.streamedValues[1].foo_id, 2001);
					assert.equal(client.importDimension.streamedValues[1].id, '_del_2001');
					assert.equal(client.importDimension.streamedValues[1].__leo_delete__, 'foo_id');
					assert.equal(client.importDimension.streamedValues[1].__leo_delete_id__, 2001);
					assert.equal(client.importDimension.streamedValues[2].foo_id, 2005);
					assert.equal(client.importDimension.streamedValues[2].id, '_del_2005');
					assert.equal(client.importDimension.streamedValues[2].__leo_delete__, 'foo_id');
					assert.equal(client.importDimension.streamedValues[2].__leo_delete_id__, 2005);
					done();
				}
			});
		});

		it('delete with id primary key', (done) => {
			const stream = es.readArray([{
				id: 'fooDeleteEventID_A',
				payload: {
					type: "delete",
					data: {
						in: [1001, 1005],
						entities: [
							{
								type: "dimension",
								table: "dim_foo_nk_single", 
								field: "id"
							}
						]
					}
				}
			},{
				id: 'fooDeleteEventID_B',
				payload: {
					type: "delete",
					data: {
						in: [1001, 1004],
						entities: [
							{
								type: "dimension",
								table: "dim_foo_nk_single", 
								field: "id"
							}
						]
					}
				}
			}]);

			load(client, tableConfig, stream, (err) => {
				if (err) {
					done(err);
				} else {
					assert.equal(client.importDimension.streamedValues[0].id, 1005);
					assert.equal(client.importDimension.streamedValues[0].__leo_delete__, 'id');
					assert.equal(client.importDimension.streamedValues[0].__leo_delete_id__, 1005);
					assert.equal(client.importDimension.streamedValues[1].id, 1001);
					assert.equal(client.importDimension.streamedValues[1].__leo_delete__, 'id');
					assert.equal(client.importDimension.streamedValues[1].__leo_delete_id__, 1001);
					assert.equal(client.importDimension.streamedValues[2].id, 1004);
					assert.equal(client.importDimension.streamedValues[2].__leo_delete__, 'id');
					assert.equal(client.importDimension.streamedValues[2].__leo_delete_id__, 1004);
					done();
				}
			});
		});

		it('delete with composite natual keys', (done) => {
			const stream = es.readArray([{
				id: 'fooDeleteEventID_A',
				payload: {
					type: 'delete',
					data: {
						in: [{ foo_pk1: '101', foo_pk2: '201' }, { foo_pk1: '100', foo_pk2: '200' }],
						entities: [
							{
								type: 'dimension',
								table: 'dim_foo'
							}
						]
					}
				}
			},{
				id: 'fooDeleteEventID_B',
				payload: {
					type: 'delete',
					data: {
						in: [{ foo_pk1: '101', foo_pk2: '201' }],
						entities: [
							{
								type: 'dimension',
								table: 'dim_foo'
							}
						]
					}
				}
			}]);

			load(client, tableConfig, stream, (err) => {
				if (err) {
					done(err);
				} else {
					assert.equal(client.importDimension.streamedValues[0].foo_pk1, '100');
					assert.equal(client.importDimension.streamedValues[0].foo_pk2, '200');
					expect(client.importDimension.streamedValues[0].__leo_delete__).to.have.members(['foo_pk1', 'foo_pk2']);
					expect(client.importDimension.streamedValues[0].__leo_delete_id__).to.deep.equals({ foo_pk1: '100', foo_pk2: '200' });
					assert.equal(client.importDimension.streamedValues[1].foo_pk1, '101');
					assert.equal(client.importDimension.streamedValues[1].foo_pk2, '201');
					expect(client.importDimension.streamedValues[1].__leo_delete__).to.have.members(['foo_pk1', 'foo_pk2']);
					expect(client.importDimension.streamedValues[1].__leo_delete_id__).to.deep.equals({ foo_pk1: '101', foo_pk2: '201' });
					done();
				}
			});
		});
	});
});


const { assert } = require('chai');
const load = require('../../datawarehouse/load');
const es = require('event-stream');

const tableConfig = {
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
	insertMissingDimensions: (usedTables, tableConfig, tableSks, tableNks, callback) => {
		console.log("INSERT MISSING DIMENSIONS usedTables", usedTables);
		console.log("INSERT MISSING DIMENSIONS tableConfig", tableConfig);
		console.log("INSERT MISSING DIMENSIONS tableSks", tableSks);
		console.log("INSERT MISSING DIMENSIONS tableNks", tableNks);
		callback();
	},
	importDimension: (stream, table, sk, nk, scds, callback) => {
		console.log("IMPORT DIMENSIONS table: ", table);
		console.log("IMPORT DIMENSIONS sk: ", sk);
		console.log("IMPORT DIMENSIONS nk: ", nk);
		console.log("IMPORT DIMENSIONS scds: ", scds);
		callback();
	},
	importFact: (stream, table, ids, callback) => {
		console.log("IMPORT FACT table: ", table);
		console.log("IMPORT FACT ids: ", ids);
		callback();
	},
	getDimensionColumn: () => {
		console.log("GET DIM COLUMN");
	}, 
	linkDimensions: (table, links, nk, callback) => {
		console.log("LINK DIMENSIONS table: ", table);
		console.log("LINK DIMENSIONS links: ", links);
		console.log("LINK DIMENSIONS nk: ", nk);
		callback();
	}
};

describe("Data Warehouse", () => {
	describe("load", () => {
		it('delete with composite natual keys', (done) => {
			const stream = es.readArray([{
				id: 'fooDeleteEventID_A',
				payload: {
					type: "delete",
					data: {
						in: [{ foo_pk1: '101', foo_pk2: '201' }, { foo_pk1: '100', foo_pk2: '200' }],
						entities: [
							{
								type: "dimension",
								table: "dim_foo", 
								field: ["foo_pk1", "foo_pk2"]
							}
						]
					}
				}
			},{
				id: 'fooDeleteEventID_B',
				payload: {
					type: "delete",
					data: {
						in: [{ foo_pk1: '101', foo_pk2: '201' }],
						entities: [
							{
								type: "dimension",
								table: "dim_foo", 
								field: ["foo_pk1", "foo_pk2"]
							}
						]
					}
				}
			}]);

			load(client, tableConfig, stream, (err) => {
				if (err) {
					assert.fail("Got Error");
					done(err);
				} else {
					done();
				}
			});
		});
	});
});

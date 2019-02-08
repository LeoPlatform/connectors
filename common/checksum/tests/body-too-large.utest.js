process.env.NODE_ENV = "dev";
//process.env.LEO_LOGGER = "/.*/tide";
let config = require("leo-config").bootstrap({
	dev: {
		leosdk: {
			LeoCron: "LeoCronTable",
			LeoSettings: "LeoSettingsTable"
		}
	}
});
require("chai").should();
const assert = require("chai").assert;
const moment = require("moment");
let leoaws = mock(require("leo-aws"), {
	dynamodb: {
		LeoCronTable: {},
		LeoSettingsTable: {}
	}
});

const checksum = require("..");

describe('checksum', function() {
	it('body-too-large error', async function() {
		this.timeout(1000 * 10);
		let options = {
			shouldDelete: true,
			sample: false,
			reverse: true,
			limit: 2,
			maxLimit: 2,
			stop_at: moment().add({
				seconds: 30
			})
		};


		let master = checksum.basicConnector("Master DB", {
			id_column: "id"
		}, ObjectConnectorMock([{
			id: 1,
			data: "hello"
		}, {
			id: 2,
			data: "hello"
		}, {
			id: 10000000
		}]));

		let slave = checksum.basicConnector("Slave DB", {
			id_column: "id"
		}, ObjectConnectorMock([{
			id: 1,
			data: "hello"
		}, ...((cnt) => {
			var a = [];
			for (var i = 2; i < cnt + 2; i++) {
				a.push({
					id: i
				});
			}
			return a;
		})(100), {
			id: 1000000,
			data: "hello2"
		}, {
			id: 10000000
		}], {
			individual: [() => {
				return Promise.reject("body too large");
			}]
		}));

		let results = await checksum.checksum("test", "test-bot-id", master, slave, options);
		//console.log(JSON.stringify(await leoaws.dynamodb.get(config.leosdk.LeoCron, "test-bot-id"), null, 2));
		assert.equal(results.totals.correct, 2, "Correct count doesn't match");
		assert.equal(results.totals.incorrect, 1, "Incorrect count doesn't match");
		assert.equal(results.totals.missing, 0, "Missing count doesn't match");
	});
});



function ObjectConnectorMock(database, config) {
	config = {
		id: "id",
		batch: [],
		individual: [],
		calls: {},
		...config
	};
	let db = database;
	if (Array.isArray(db)) {
		let tmp = {};
		db.map(r => {
			tmp[r[config.id]] = r;
		});
		db = tmp;
	}
	let obj = {
		getConfig: function() {
			return config;
		},
		// Respond to start and end
		// Return Stream, Array, or a Hash
		batch: function(start, end) {
			//console.log(`batch: ${start}, ${end}`);
			let data = [];
			for (let i = start; i <= end; i++) {
				let v = db[i];
				if (v !== undefined) {
					data.push(v);
				}
			}
			return Promise.resolve(data);
		},

		// Respond to start and end
		// Return Stream, Array, or a Hash
		individual: function(start, end) {
			//console.log(`individual: ${start}, ${end}`);
			let data = [];
			for (let i = start; i <= end; i++) {
				let v = db[i];
				if (v !== undefined) {
					data.push(v);
				}
			}
			return Promise.resolve(data);
		},

		// Respond to ids
		// Return Stream, Array
		sample: function(ids) {
			//console.log(`sample: ${ids}`);
			let data = [];
			ids.map(id => {
				let v = db[id];
				if (v !== undefined) {
					data.push(v);
				}
			});

			return Promise.resolve(data);
		},

		// Respond to start and end -- options
		// Return object with min, max, total
		range: function(start, end) {

			//console.log(`range: ${start}, ${end}`);
			let min = null;
			let max = null;
			let total = 0;
			Object.keys(db).map(id => {
				id = db[id][this.settings.id_column];
				if ((start == undefined || id >= start) && (end == undefined || id <= end)) {
					total++;
					if (min == null || id < min) {
						min = id;
					}
					if (max == null || id > max) {
						max = id;
					}
				}
			});
			return Promise.resolve({
				min,
				max,
				total
			});
		},

		// Responds to start, end, limit, reverse
		// Returns object with next, current
		nibble: function(start, end, limit, reverse) {

			//console.log(`nibble: ${start}, ${end}, ${limit}, ${reverse}`);
			let current = null;
			let next = null;
			let dir = 1;
			let ostart = start;
			let oend = end;
			if (reverse) {
				start = end;
				end = ostart;
				dir = -1;
			}
			let cnt = 0;
			for (let i = start; i >= ostart && i <= oend; i += dir) {
				let v = db[i];
				if (v !== undefined) {
					cnt++;
					if (cnt >= limit) {
						if (!current) {
							current = v[this.settings.id_column];
						} else {
							next = v[this.settings.id_column];
							break;
						}
					}
				}
			}

			return Promise.resolve({
				current,
				next
			});
		},

		// Called With data
		// Returns a session
		initialize: function(data) {
			//console.log(`initialize: ${JSON.stringify(data,null,2)}`);
			return Promise.resolve({});
		},

		// Called With data
		destroy: function(data) {
			//console.log(`destroy: ${JSON.stringify(data,null,2)}`);
			return Promise.resolve();
		},

		// Respond to ids
		// No return
		delete: function(ids) {
			//console.log(`delete: ${ids}, not: ${this.not_ids} start: ${this.start} end:${this.end}`);

			(ids || []).map(id => {
				if (id in db) {
					delete db[id];
				}
			});

			if (this.not_ids && this.start && this.end) {
				let not = {};
				(this.not_ids || []).map(id => not[id] = true);
				for (let i = this.start; i <= this.end; i++) {
					if (!(i in not)) {
						delete db[i];
					}
				}
			}

			return Promise.resolve();
		},
	};

	Object.keys(obj).map(key => {
		let fn = obj[key];
		obj[key] = function(...args) {
			let index = config.calls[key] || 0;
			config.calls[key] = index + 1;
			if (config[key] && config[key][index]) {
				return config[key][index].apply(this, args);
			} else {
				return fn.apply(this, args);
			}
		};
	});

	return obj;
}


let merge = require("lodash.merge");

function mock(leoaws, options = {}) {
	let db = Object.assign({}, options.dynamodb);
	leoaws.dynamodb = {
		get: function(table, id, opts = {}) {
			if (!(table in db)) {
				return Promise.reject(new Error("Table does not exist:" + table))
			}
			if (id in db[table]) {
				return Promise.resolve(db[table][id]);
			}
			return Promise.resolve();
		},
		merge: function(table, id, data) {
			if (!(table in db)) {
				db[table] = {}
			}
			merge(db[table], {
				[id]: data
			});
			return Promise.resolve({})
		},
		getDB: function() {
			return db;
		}
	}
	return leoaws;
}

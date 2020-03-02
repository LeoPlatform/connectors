const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const extend = require("extend");
const dynamodb = leo.aws.dynamodb;
const merge = require("lodash.merge");
const leoaws = require('leo-aws');
const logger = require('leo-logger');

let aggregations = {};

let bucketAliases = {
	'alltime': '',
	'secondly': 'YYYY-MM-DD HH:mm:ss',
	'minutely': 'YYYY-MM-DD HH:mm',
	'hourly': 'YYYY-MM-DD HH',
	'monthly': 'YYYY-MM',
	'daily': 'YYYY-MM-DD',
	'weekly': 'YYYY-W',
	'quarterly': 'YYYY-Q'
};
let defaultTypes = {
	'sum': {
		v: 0
	},
	'min': {
		v: null
	},
	'max': {
		v: null
	}
};

function processUpdate(newData, existing, reversal) {
	for (let key in newData) {
		let func = newData[key];
		if (func && func._type) {
			func = merge({}, defaultTypes[func._type], func);
			if (!reversal) {
				if (!(key in existing)) {
					existing[key] = func;
				} else if ("_type" in func) {
					if (func._type === "sum") {
						existing[key].v += func.v;
					} else if (func._type === "min") {
						if (func.v < existing[key].v) {
							existing[key].v = func.v;
						}
					} else if (func._type === "max") {
						if (func.v > existing[key].v) {
							existing[key].v = func.v;
						}
					} else if (func._type === "last") {
						if (func.date > existing[key].date) {
							existing[key] = func;
						}
					} else if (func._type === "first") {
						if (func.date < existing[key].date) {
							existing[key] = func;
						}
					} else if (func._type === "changes") {
						if (existing[key].prev !== func.prev) {
							existing[key].v++;
						}
						existing[key].prev = func.prev;
					}
				}
			} else { //reversal
				if (!(key in existing) && ["sum"].indexOf(func._type) !== -1) {
					existing[key] = {
						_type: func._type,
						v: 0
					};
				}
				if (func._type === "sum") {
					existing[key].v -= func.v;
				}
			}
		} else {
			existing[key] = existing[key] || {};
			processUpdate(func, existing[key], reversal);
		}
	}
}

function myProcess(ns, e, reversal) {
	let id = e.entity + "-" + e.id;
	if (ns) {
		ns = "-" + ns;
	}
	let buckets = [];
	if (!e.aggregate) {
		buckets = [{
			cat: "all",
			range: ""
		}];
	} else {
		let d = moment(e.aggregate.timestamp);
		e.aggregate.buckets.forEach((bucket) => {
			let bucketObj = {
				cat: bucket,
				range: ''
			};
			let bucketLower = bucket.toLowerCase();

			if (bucketLower in bucketAliases) {
				bucketObj.range = d.format(bucketAliases[bucketLower]);
			} else if (bucket === "" || bucket === "alltime" || bucket === "all") {
				bucketObj.cat = 'all';
			} else {
				for (let i in bucketAliases) {
					// do not change case, since time formats are case sensitive
					if (bucket === bucketAliases[i]) {
						bucketObj.cat = i;
						bucketObj.range = d.format(i);
						break;
					}
				}
			}

			buckets.push(bucketObj);
		});
	}
	buckets.forEach((bucket) => {
		let data = merge({}, e.data);
		let newId = id + "-" + bucket.cat;
		let range = bucket.range + ns;
		if (bucket.cat === "all") {
			newId = id;
			range = "all" + ns;
		}

		let fullHash = newId + range;
		if (!(fullHash in aggregations)) {
			aggregations[fullHash] = {
				id: newId,
				bucket: range,
				d: {}
			};
		}
		processUpdate(data, aggregations[fullHash].d, reversal);
	});
}


module.exports = {
	sum: (v) => ({
		_type: 'sum',
		v: v
	}),
	min: (v) => ({
		_type: 'min',
		v: v
	}),
	max: (v) => ({
		_type: 'max',
		v: v
	}),
	countChanges: (v) => ({
		_type: 'changes',
		prev: v,
		v: 0
	}),
	last: (date, v) => ({
		_type: 'last',
		date: date,
		v: v
	}),
	first: (date, v) => ({
		_type: 'first',
		date: date,
		v: v
	}),
	hash: (key, func) => {
		let hash = {};
		this.forEach((e) => {
			hash[e[key]] = func(e);
		});
		return hash;
	},
	/**
	 *
	 * @param tableName
	 * @param ns entity
	 * @param t transform function
	 * @returns {*}
	 */
	aggregator: function(tableName, ns, t) {
		if (!t) {
			t = ns;
			ns = "";
		}

		let start = null;
		aggregations = {};
		return ls.bufferBackoff(function each(obj, done) {
			if (!start) start = obj.eid;

			obj = obj.payload;
			if (obj.old) {
				t(obj.old).forEach((e) => {
					myProcess(ns, e, true);
				});
			}
			if (obj.new) {
				t(obj.new).forEach((e) => {
					myProcess(ns, e, false);
				});
			}
			done();
		},
		function emit(records, done) {
			//Fetch Ids
			let ids = Object.keys(aggregations)
				.map(hash => ({
					id: aggregations[hash].id,
					bucket: aggregations[hash].bucket
				}));

			let stream = leo.streams.toDynamoDB(tableName, {
				records: 500
			});
			let seenHashes = {};

			dynamodb.batchGetTable(tableName, ids, (err, result) => {
				result.forEach(record => {
					let fullHash = record.id + record.bucket;
					seenHashes[fullHash] = true;
					if (start === record.start) {
						record.d = record.p || {};
					}
					record.p = extend(true, {}, record.d);
					record.start = start;

					processUpdate(aggregations[fullHash].d, record.d, false);

					stream.write(record);
				});

				Object.keys(aggregations)
					.filter(hash => !(hash in seenHashes))
					.map(hash => (Object.assign({
						p: {},
						start: start
					}, aggregations[hash])))
					.forEach(a => stream.write(a));

				stream.end((err) => {
					start = null;
					aggregations = {};
					done(err, []);
				});
			});
		}, {}, {
			records: 1000
		});
	},
	/**
	 * Get current value from aggregate data
	 * If no key is passed, returns entire current object.
	 * @param obj (aggregate data object)
	 * @param key (name of child object you want to look in)
	 * @returns {*|{}}
	 */
	getCurrent: function(obj, key) {
		if (key) {
			return obj.d[key] && obj.d[key].v || {};
		}

		return obj.d;
	},
	/**
	 * Get meta data from aggregate data.
	 * If no key is passed, returns entire current object.
	 * @param obj
	 * @param key
	 * @returns {{}}
	 */
	getCurrentMeta: function(obj, key) {
		if (key) {
			return obj.d[key] || {};
		}

		return obj.d;
	},
	/**
	 * Get previous value from aggregate data
	 * If no key is passed, returns entire previous object.
	 * @param obj (aggregate data object)
	 * @param key (name of child object you want to look in)
	 * @returns {*|{}}
	 */
	getPrevious: function(obj, key) {
		if (key) {
			return obj.p[key] && obj.p[key].v || {};
		}

		return obj.p;
	},
	/**
	 * Get meta data from aggregate data.
	 * If no key is passed, returns entire previous object.
	 * @param obj
	 * @param key
	 * @returns {{}}
	 */
	getPreviousMeta: function(obj, key) {
		if (key) {
			return obj.p[key] || {};
		}

		return obj.p;
	},
	/**
	 * query wrapper for the aggregations table
	 * @param table
	 * @param items
	 * @param opts
	 * @returns {Promise<any[]>}
	 */
	query: async function(table, items, opts) {
		opts = merge({
			start: null,
			end: null,
			offset: null,
			limit: null
		}, opts);

		let defaultQuery = {
			TableName: table,
			KeyConditionExpression: '#id = :id',
			ExpressionAttributeNames: {
				"#id": "id"
			},
			ExpressionAttributeValues: {
				":id": ''
			},
		};
		let configuration = {};
		if (opts.limit) {
			defaultQuery.Limit = opts.limit;
		}

		let tasks = [];

		if (Array.isArray(items)) {
			items.forEach(item => {
				// build ID from items
				let id = [];
				if (typeof item === 'string') {
					id.push(item);
				} else {
					if (item.prefix) {
						id.push(item.prefix);
					}
					if (item.id) {
						id.push(item.id);
					}
					if (opts.frequency) {
						id.push(opts.frequency);
					}
				}

				// clone defaultQuery so we can reuse the object if we have selected multiple items
				let query = Object.assign({}, defaultQuery);
				query.ExpressionAttributeValues[':id'] = id.join('-');

				// do we have a start/end time?
				if (opts.start && opts.end) {
					query.KeyConditionExpression += ' AND #bucket BETWEEN :start AND :end';
					query.ExpressionAttributeNames['#bucket'] = 'bucket';
					query.ExpressionAttributeValues[':start'] = opts.start;
					query.ExpressionAttributeValues[':end'] = opts.end;
				} else if (opts.start) {
					query.KeyConditionExpression += ' AND #bucket >= :bucket';
					query.ExpressionAttributeNames['#bucket'] = 'bucket';
					query.ExpressionAttributeValues[':bucket'] = opts.start;
				} else if (opts.end) {
					query.KeyConditionExpression += ' AND #bucket <= :bucket';
					query.ExpressionAttributeNames['#bucket'] = 'bucket';
					query.ExpressionAttributeValues[':bucket'] = opts.end;
				}

				tasks.push(new Promise(resolve => {
					logger.log('DynamoDB Query', query);

					leoaws.dynamodb.smartQuery(query, configuration).then(result => {
						resolve(result);
					}).catch(err => {
						throw new Error(err);
					});
				}));
			});
		}

		return await Promise.all(tasks);
	}
};

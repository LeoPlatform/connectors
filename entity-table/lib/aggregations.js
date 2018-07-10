const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const extend = require("extend");
const dynamodb = leo.aws.dynamodb;
const merge = require("lodash.merge");

let start = null;
var aggregations = {};

let bucketAliases = {
	'alltime': '',
	'monthly': 'YYYY-MM',
	'daily': 'YYYY-MM-DD',
	'weekly': 'YYYY-W',
	'quarterly': 'YYYY-Q'
};
let defaultTypes = {
	'sum': {
		val: 0
	},
	'min': {
		val: null
	},
	'max': {
		val: null
	}
};

function processUpdate(newData, existing, reversal) {
	for (let key in newData) {
		let func = newData[key];
		if (func._type) {
			func = merge({}, defaultTypes[func._type], func);
			if (!reversal) {
				if (!(key in existing)) {
					existing[key] = func;
				} else if ("_type" in func) {
					if (func._type == "sum") {
						existing[key].val += func.val;
					} else if (func._type == "min") {
						if (func.val < existing[key].val) {
							existing[key].val = func.val;
						}
					} else if (func._type == "max") {
						if (func.val > existing[key].val) {
							existing[key].val = func.val;
						}
					} else if (func._type == "last") {
						if (func.date > existing[key].date) {
							existing[key] = func;
						}
					} else if (func._type == "first") {
						if (func.date < existing[key].date) {
							existing[key] = func;
						}
					} else if (func._type == "changes") {
						if (existing[key].prev != func.prev) {
							existing[key].val++;
						}
						existing[key].prev = func.prev;
					}
				}
			} else { //reversal
				if (!(key in existing) && ["sum"].indexOf(func._type) !== -1) {
					existing[key] = {
						_type: func._type,
						val: 0
					};
				}
				if (func._type == "sum") {
					existing[key].val -= func.val;
				}
			}
		} else {
			existing[key] = existing[key] || {};
			processUpdate(func, existing[key], reversal);
		}
	}
}

function myProcess(ns, e, reversal) {
	var id = e.entity + "-" + e.id;
	if (ns) {
		ns = "-" + ns;
	}
	var buckets = [];
	if (!e.aggregate) {
		buckets = [{
			cat: "all",
			range: ""
		}];
	} else {
		var d = moment(e.aggregate.timestamp);
		e.aggregate.buckets.forEach((bucket) => {
			if (bucket == "" || bucket == "alltime" || bucket == "all") {
				buckets.push({
					cat: "all",
					range: ""
				});
			} else {
				if (bucket.toLowerCase() in bucketAliases) {
					bucket = bucketAliases[bucket.toLowerCase()];
				}
				buckets.push({
					cat: bucket,
					range: d.format(bucket)
				});
			}
		});
	}
	buckets.forEach((bucket) => {
		var data = merge({}, e.data);
		var newId = id + "-" + bucket.cat;
		let range = bucket.range + ns;
		if (bucket.cat == "all") {
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
	sum: (val) => ({
		_type: 'sum',
		val: val
	}),
	min: (val) => ({
		_type: 'min',
		val: val
	}),
	max: (val) => ({
		_type: 'max',
		val: val
	}),
	countChanges: (val) => ({
		_type: 'changes',
		prev: val,
		val: 0
	}),
	last: (date, values) => ({
		_type: 'last',
		date: date,
		values: values
	}),
	first: (date, values) => ({
		_type: 'first',
		date: date,
		values: values
	}),
	hash: (key, func) => {
		var hash = {};
		this.forEach((e) => {
			hash[e[key]] = func(e);
		});
		return hash;
	},
	aggregator: function(ns, t) {
		if (!t) {
			t = ns;
			ns = "";
		}
		return ls.bufferBackoff(function each(obj, done) {
				if (!start) start = obj.eid;

				obj = obj.payload;
				let changes = {};
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
				let ids = [];
				for (var i in aggregations) {
					ids.push({
						id: aggregations[i].id,
						bucket: aggregations[i].bucket
					});
				}
				// console.log(ids);
				let stream = leo.streams.toDynamoDB("aggregations");
				let seenHashes = {};

				dynamodb.batchGetTable("aggregations", ids, (err, result) => {
					for (var i = 0; i < result.length; i++) {
						let record = result[i];
						let fullHash = record.id + record.bucket;
						seenHashes[fullHash] = true;
						if (start == record.start) {
							record.d = record.p || {};
						}
						record.p = extend(true, {}, record.d);
						record.start = start;

						processUpdate(aggregations[fullHash].d, record.d, false);

						stream.write(record);
					}
					for (var i in aggregations) {
						if (!(i in seenHashes)) {
							aggregations[i].p = {};
							aggregations[i].start = start;
							// console.log("---aggregations");
							// console.log(aggregations[i]);
							stream.write(aggregations[i]);
						}
					}
					stream.end((err) => {
						done(err, []);
					});
				});
			}, {}, {});
	}
};

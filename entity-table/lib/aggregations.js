const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const extend = require("extend");
const dynamodb = leo.aws.dynamodb;

let start = null;
var aggregations = {};

function processUpdate(newData, existing, reversal, prev = {}) {
	for (let key in newData) {
		let func = newData[key];
		if (func._type) {
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

function myProcess(e, reversal, prev) {
	var id = e.entity + "-" + e.id;
	var groups = [];
	if (!e.bucket) {
		groups = [{
			cat: "all",
			range: ""
		}];
	} else {
		var d = moment(e.bucket.date);
		e.bucket.groups.forEach((g) => {
			if (g == "") {
				groups.push({
					cat: "all",
					range: ""
				});
			} else {
				groups.push({
					cat: g,
					range: d.format(g)
				});
			}
		});
	}

	//@todo do a proper clone or figure out what this should be
	var tempData = JSON.stringify(e.data);

	groups.forEach((g) => {
		var data = JSON.parse(tempData);
		var newId = id + "-" + g.cat;
		let bucket = g.range;
		if (g.cat == "all") {
			newId = id;
			bucket = "all";
		}

		let fullHash = newId + bucket;
		if (!(fullHash in aggregations)) {
			aggregations[fullHash] = {
				id: newId,
				bucket: bucket,
				d: {}
			};
		}
		processUpdate(data, aggregations[fullHash].d, reversal, prev);
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
	pr: function(t) {
		return ls.bufferBackoff(function each(obj, done) {
				if (!start) start = obj.eid;

				obj = obj.payload;
				let changes = {};
				if (obj.old) {
					t(obj.old).forEach((e) => {
						if (e.valid) {
							myProcess(e, true);
						}
					});
				}
				if (obj.new) {
					t(obj.new).forEach((e) => {
						if (e.valid) {
							myProcess(e, false);
						}
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
				console.log(ids);
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
							console.log("---aggregations");
							console.log(aggregations[i]);
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

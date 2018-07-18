const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const extend = require("extend");
const dynamodb = leo.aws.dynamodb;
const merge = require("lodash.merge");

let aggregations = {};

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
                    if (func._type === "sum") {
                        existing[key].val += func.val;
                    } else if (func._type === "min") {
                        if (func.val < existing[key].val) {
                            existing[key].val = func.val;
                        }
                    } else if (func._type === "max") {
                        if (func.val > existing[key].val) {
                            existing[key].val = func.val;
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
                if (func._type === "sum") {
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
            if (bucket === "" || bucket === "alltime" || bucket === "all") {
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
        let hash = {};
        this.forEach((e) => {
            hash[e[key]] = func(e);
        });
        return hash;
    },
    aggregator: function (tableName, ns, t) {
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

                let stream = leo.streams.toDynamoDB(tableName);
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
                        .map(hash => (Object.assign({p: {}, start: start}, aggregations[hash])))
                        .forEach(a => stream.write(a));

                    stream.end((err) => {
                        start = null;
                        aggregations = {};
                        done(err, []);
                    });
                });
            }, {}, {});
    }
};

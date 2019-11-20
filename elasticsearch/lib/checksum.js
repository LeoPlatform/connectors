"use strict";
const crypto = require('crypto');
var base = require("leo-connector-common/checksum/lib/handler.js");
var moment = require("moment");

var scroll = "15s";
module.exports = function(client) {
	return base({
		batch: batch,
		individual: individual,
		sample: sample,
		nibble: nibble,
		range: range,
		initialize: initialize,
		destroy: destroy,
	});

	function getClient(settings) {
		return client;
	}

	function batch(event, callback) {
		console.log("Calling Batch", JSON.stringify(event, null, 2));
		//console.time("search");
		var es = getClient(event.settings);
		var checksum = event.data;
		var settings = event.settings;
		var fields = settings.fields;
		var results = {
			qty: 0,
			hash: [0, 0, 0, 0]
		}
		var query = {
			"range": {
				[settings.id_column]: {
					"gte": checksum.start,
					"lte": checksum.end
				}
			}
		};

		if (settings.query) {
			query = {
				bool: {
					must: [settings.query, query]
				}
			};
		}

		var params = {
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: [{
					[settings.id_column]: "desc"
				}],
				_source: fields,
				size: 10000
			},
			scroll: scroll,
			track_total_hits: settings.track_total_hits
		};
		//console.log(JSON.stringify(params, null, 2))
		var s = es.search(params, function getUntilDone(err, result) {
			//console.timeEnd("search")
			if (err) {
				console.log(err);
				callback(err);
				return;
			}
			result.hits.hits.map(item => {
				item._source.id = item._id;
				var hash = calcChecksum(item._source, fields);
				//console.log(JSON.stringify(item, null, 2))
				results.hash[0] += hash.readUInt32BE(0);
				results.hash[1] += hash.readUInt32BE(4);
				results.hash[2] += hash.readUInt32BE(8);
				results.hash[3] += hash.readUInt32BE(12);
				results.qty += 1;
			});

			if (scroll && (((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total) !== results.qty) {
				es.scroll({
					scrollId: result._scroll_id,
					scroll: scroll
				}, getUntilDone)
			} else {
				//console.log("Total Hits:", ((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total))
				callback(null, {
					ids: checksum.ids,
					start: checksum.start,
					end: checksum.end,
					qty: results.qty,
					hash: results.hash
				});
			};
		});
	}

	function individual(event, callback) {
		console.log("Calling Individual", JSON.stringify(event, null, 2));

		var checksum = event.data;
		var settings = event.settings;
		var fields = settings.fields;

		var es = getClient(settings);

		var results = {
			ids: checksum.ids,
			start: checksum.start,
			end: checksum.end,
			qty: 0,
			checksums: []
		};

		var id = settings.id_column == "_id" ? (i) => {
			return i._id
		} : (i) => {
			return i._source[settings.id_column] || i._source[settings.id_column.replace(".keyword", "")]
		};

		var _id = settings._id_column == "_id" ? (i) => {
			return i._id
		} : (i) => {
			return settings._id_column ? (i._source[settings._id_column] || i._source[settings._id_column.replace(".keyword", "")]) : undefined
		};

		var query = {
			"range": {
				[settings.id_column]: {
					"gte": checksum.start,
					"lte": checksum.end
				}
			}
		};
		if (settings.query) {
			query = {
				bool: {
					must: [settings.query, query]
				}
			};
		}

		var sourceFields = settings._id_column ? [settings._id_column].concat(fields) : fields
		var s = es.search({
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: [{
					[settings.id_column]: "desc"
				}],
				_source: sourceFields,

				size: 10000
			},
			scroll: scroll,
			track_total_hits: settings.track_total_hits
		}, function getUntilDone(err, result) {
			//console.timeEnd("search")
			if (err) {
				console.log(err)
				callback(err);
				return;
			}
			result.hits.hits.map(item => {
				results.qty++;
				item._source.id = item._id;
				results.checksums.push({
					id: id(item),
					_id: _id(item),
					hash: calcChecksum(item._source, fields, "hex")
				})
			});

			if (scroll && (((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total) !== results.qty) {
				es.scroll({
					scrollId: result._scroll_id,
					scroll: scroll
				}, getUntilDone)
			} else {
				//console.log("Total Hits:", ((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total))
				callback(null, results);
			};
		});
	}

	function sample(event, callback) {
		console.log("Calling Sample", JSON.stringify(event, null, 2));

		var es = getClient(event.settings);
		var checksum = event.data;
		var settings = event.settings;
		var fields = settings.fields;

		var query = query = {
			bool: {
				filter: {
					terms: {
						[settings.id_column]: checksum.ids
					}
				}
			}
		};
		if (settings.query) {
			query = {
				bool: {
					must: [settings.query, query]
				}
			};
		}
		var results = [];
		var s = es.search({
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: [{
					[settings.id_column]: "asc"
				}],
				_source: fields,
				size: 10000
			},
			scroll: scroll,
			track_total_hits: settings.track_total_hits
		}, function getUntilDone(err, result) {
			//console.timeEnd("search")
			if (err) {
				console.log(err);
				callback(err);
				return;
			}

			result.hits.hits.map(item => {
				item._source.id = item._id;
				results.push(item);
			});

			if (scroll && (((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total) !== results.length) {
				es.scroll({
					scrollId: result._scroll_id,
					scroll: scroll
				}, getUntilDone)
			} else {
				var a = {
					ids: checksum.ids,
					start: checksum.start,
					end: checksum.end,
					qty: results.length,
					checksums: results.map(item => {
						//console.log(JSON.stringify(item, null, 2));
						return fields.map(field => {
							var value = item._source[field];
							if (value instanceof Date) {
								return Math.round(value.getTime() / 1000) + "  " + moment(value).utc().format();
							} else if (value && typeof value == "object" && value.toHexString) {
								return value.toString();
							} else if (value && value.match && value.match(/[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}/)) {
								var m = moment(value);
								return Math.round(m.valueOf() / 1000) + "  " + moment(m).utc().format();
							} else {
								return value;
							}
						});
					})
				}

				//console.log("Total Hits:", ((typeof result.hits.total === 'object') ? result.hits.total.value : result.hits.total), fields)
				//console.log(JSON.stringify(a, null, 2));
				callback(null, a);
			};
		});
	}

	function range(event, callback) {
		console.log("Calling Range", JSON.stringify(event, null, 2));
		var es = getClient(event.settings);
		var settings = event.settings

		var query = {
			"match_all": {}
		};

		if (settings.query) {
			query = {
				bool: {
					must: [settings.query, query]
				}
			};
		}

		console.log({
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: {
					[settings.id_column]: "asc"
				},
				_source: [settings.id_column],
				size: 1
			}
		})
		var s = es.search({
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: {
					[settings.id_column]: "asc"
				},
				_source: [settings.id_column],
				size: 1
			},
			track_total_hits: settings.track_total_hits
		}, (err, min) => {
			if (err) {
				console.log(err);
				callback(err);
				return;
			}

			es.search({
				index: settings.index,
				type: settings.type,
				body: {
					query: query,
					sort: {
						[settings.id_column]: "desc"
					},
					_source: [settings.id_column],
					size: 1
				},
				track_total_hits: settings.track_total_hits
			}, (err, max) => {
				if (err) {
					console.log(err);
					callback(err);
					return;
				}

				var id = settings.id_column == "_id" ? (i) => {
					return i._id
				} : (i) => {
					return i._source[settings.id_column] || i._source[settings.id_column.replace(".keyword", "")]
				};

				callback(null, {
					min: id(min.hits.hits[0]),
					max: id(max.hits.hits[0]),
					total: ((typeof min.hits.total === 'object') ? min.hits.total.value : min.hits.total)
				});
			});

		});
	}

	function nibble(event, callback) {
		console.log("Calling Nibble", JSON.stringify(event, null, 2));
		var data = event.data;
		var settings = event.settings;

		var es = getClient(settings);
		var query = {};
		if (data.start) {
			query.gte = data.start;
		}
		if (data.end) {
			query.lte = data.end;
		}
		query = {
			range: {
				[settings.id_column]: query
			}
		};
		if (settings.query) {
			query = {
				bool: {
					must: [settings.query, query]
				}
			};
		}
		es.search({
			index: settings.index,
			type: settings.type,
			body: {
				query: query,
				sort: {
					[settings.id_column]: !data.reverse ? "asc" : "desc"
				},
				_source: [settings.id_column],
				size: 2,
				from: data.limit - 1
			},
			track_total_hits: settings.track_total_hits
		}, (err, result) => {
			if (err) {
				console.log(err);
				callback(err);
				return;
			}
			var id = settings.id_column == "_id" ? (i) => {
				return i._id
			} : (i) => {
				return i._source[settings.id_column] || i._source[settings.id_column.replace(".keyword", "")]
			};

			data.current = result.hits.hits[0] ? id(result.hits.hits[0]) : null;
			data.next = result.hits.hits[1] ? id(result.hits.hits[1]) : null;
			callback(null, data);
		});
	}

	function initialize(event, callback) {
		console.log("Calling Initialize", JSON.stringify(event, null, 2));
		callback(null, {});
	}

	function destroy(event, callback) {
		console.log("Calling Destroy", JSON.stringify(event, null, 2));
		callback();
	}

	function calcChecksum(item, fields, method) {
		var allFields = "";
		fields.forEach(field => {
			var value = item[field];
			if (value instanceof Date) {
				allFields += crypto.createHash('md5').update(Math.round(value.getTime() / 1000).toString()).digest('hex');
			} else if (value && value.match && value.match(/[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}/)) {
				allFields += crypto.createHash('md5').update(Math.round(moment(value) / 1000).toString()).digest('hex');
			} else if (value !== null && value !== undefined && value.toString) {
				allFields += crypto.createHash('md5').update(value.toString()).digest('hex');
			} else {
				allFields += " ";
			}
		});

		return crypto.createHash('md5').update(allFields).digest(method);
	}
}

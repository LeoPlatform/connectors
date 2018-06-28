"use strict";
const leo = require("leo-sdk");
const ls = leo.streams;

const {
	sum,
	min,
	last,
	first,
	hash,
	countChanges,
	pr
} = require("leo-connector-entity-table/lib/aggregations.js");

exports.handler = require("leo-sdk/wrappers/cron")(function(event, context, callback) {
	let settings = Object.assign({
		source: 'Order_changes'
	}, event);
	let t = function($) {
		return [{
			entity: 'order',
			id: $.id,
			bucket: {
				date: $._dates.created,
				groups: ["", "YYYY-MM"]
			},
			valid: ["refunded", "cancelled", "pending", "pending_with_errors"].indexOf($.status) === -1,
			data: {
				order: {
					count: sum(1),
					days_to_arrive: sum($.days_to_arrive),
					count_changes: countChanges($.days_to_arrive)
				}
			}
		}];
	}
	ls.pipe(leo.read(context.botId, settings.source, {
		start: 'z/2018/06/28/13/13/1530191603845-0000001'
	}), pr(t), (err) => {
		console.log("DONE");
		callback(err);
	});
});

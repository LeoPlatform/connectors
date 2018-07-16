"use strict";
const leo = require("leo-sdk");
const ls = leo.streams;

const {
	sum,
	min,
	max,
	last,
	first,
	hash,
	countChanges,
	aggregator
} = require("leo-connector-entity-table/lib/aggregations.js");

exports.handler = require("leo-sdk/wrappers/cron")(function(event, context, callback) {
	let settings = Object.assign({
		source: 'Order_changes'
	}, event);


	ls.pipe(leo.read(context.botId, settings.source), aggregator('order', ($) => {
		let aggregates = [];

		//Should this order be aggregated?
		if (!["refunded", "cancelled", "pending", "pending_with_errors"].includes($.status)) {

			//I want to track how many times the delivery estimate changes per order
			aggregates.push({
				entity: 'order',
				id: $.id,
				aggregate: {
					timestamp: $._dates.created,
					buckets: ["alltime"]
				},
				data: {
					count_changes: countChanges($.days_to_arrive)
				}
			});

			//I want to track user ordering statistics
			aggregates.push({
				entity: 'user',
				id: $.id,
				aggregate: {
					timestamp: $._dates.shipped,
					buckets: ["alltime", "weekly"]
				},
				data: {
					order: {
						count: sum(1),
						total: sum($.total),
						lastOrder: last($._dates.shipped, {
							id: $.id,
							total: $.total
						})
					}
				}
			});
		}
		return aggregates;
	}), (err) => {
		console.log("DONE");
		callback(err);
	});
});

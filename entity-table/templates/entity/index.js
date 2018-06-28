"use strict";

let config = require("leo-config");
let entityTable = require("leo-connector-entity-table");

exports.handler = require("leo-sdk/wrappers/cron")(function(event, context, callback) {
	let table = config.entityTableName;
	let queue = "Order";

	entityTable.loadFromQueue(table, queue, (obj, hash) => {
		return Object.assign(obj.fullobj, {
			partition: hash('Order', parseInt(obj.fullobj._id)),
			id: parseInt(obj.fullobj._id),
		});
	}, {
		botId: context.botId,
		merge: false,
	}).then(() => {
		console.log(`Completed. Remaining Time:`, context.getRemainingTimeInMillis());
		callback();
	}).catch(callback);
});

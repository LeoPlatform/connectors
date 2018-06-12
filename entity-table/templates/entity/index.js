"use strict";

let config = require("leo-config");
let entityTable = require("leo-connector-entity-table");

exports.handler = function(event, context, callback) {
	let table = config.entityTableName;
	let queue = "____ENTITY____";

	entityTable.loadFromQueue(table, queue, obj => obj.id, {
		botId: context.botId,
		data_field: obj => obj,
		batchRecords: 25,
		merge: false
	}).then(() => {
		console.log(`Completed. Remaining Time:`, context.getRemainingTimeInMillis());
		callback();
	}).catch(callback);
}
"use strict";

let config = require("leo-config");
let entityTable = require("leo-connector-entity-table");
const queueName = "__source_queue__";

exports.handler = require("leo-sdk/wrappers/cron.js")((event, context, callback) => {
	const payloadTransform = (payload, hash) => {
		let hashed = hash(queueName, payload.id);

		return Object.assign({}, payload, {
			id: __id_parsed__,
			partition: hashed
		});
	};

	entityTable.loadFromQueue(config.__Entities_Ref__, queueName, payloadTransform, {
		botId: context.botId,
		batchRecords: 25,
		merge: false
	}).then(() => {
		console.log(`Completed. Remaining Time:`, context.getRemainingTimeInMillis());
		callback();
	}).catch(callback);
});

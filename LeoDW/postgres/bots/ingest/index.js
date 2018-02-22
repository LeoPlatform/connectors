"use strict";

const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const combine = require("../../../lib/combine.js");
const fs = require("fs");

exports.handler = function(event, context, callback) {
	const ID = event.botId;
	let stats = ls.stats(event.botId, "dw.loadv2");

	let client = require("../../../../postgres/lib/connect.js")({
		user: 'root',
		host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
		database: 'sourcedata',
		password: 'Leo1234TestPassword',
		port: 5432,
	});

	ls.pipe(
		leo.read(ID, "dw.loadv2", {
			limit: 1000,
			stopTime: moment().add(240, "seconds")
		}), stats, combine((table, fieldList) => {
			return client.streamToTable(table, fieldList);
		}),
		ls.through((obj, done) => {
			console.log(obj);
			done();
		}),
		(err) => {
			client.disconnect();
			console.log("in here");
			console.log(err);
			if (!err) {
				stats.checkpoint(callback);
			} else {
				callback(err);
			}
		}
	);

};
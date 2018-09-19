"use strict";
const leo = require("leo-sdk");
const ls = require("leo-streams");
const config = require("leo-config");
const db = require("leo-connector-mysql");

exports.handler = require("leo-sdk/wrappers/cron")(async function(event, context, callback) {
	const ID = context.botId;
	let settings = Object.assign({
		source: "system:mysql",
		destination: "mysql_changes",
		reportEvery: 1000,
		runAgain: true,
		duration: context.getRemainingTimeInMillis() * 0.8
	}, event);
	let connectionInfo = config.mySqlConnectionInfo;

	//Advanced Settings
	// Run Forever
	settings.runAgain && leo.bot.runAgain();
	ls.pipe(
		db.streamChanges(
			Object.assign({
				port: 3306,
				connectionLimit: 10,
			}, connectionInfo), {
				position: await leo.bot.getCheckpoint(settings.source),
				duration: settings.duration,
				source: settings.source
			}),
		ls.count(settings.reportEvery),
		leo.load(ID, settings.destination),
		(err) => {
			err && console.log(err);
			callback(err);
		}
	);
	//End Advanced Settings
});

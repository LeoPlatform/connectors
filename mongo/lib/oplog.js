"use strict";

const leo = require('leo-sdk');
const cron = leo.bot;
const connector = require('./tail');
const ls = require('leo-streams');

exports.handler = function (settings, context, callback) {
	console.log(settings.source, settings.botId, settings.destination);
	var stream = leo.load(settings.botId, settings.destination, settings.loadOpts);
	settings.__tail = connector.stream(settings);
	ls.pipe(settings.__tail, ls.through((obj, done) => {
		if (!stream.write(obj)) {
			stream.once("drain", () => {
				done();
			});
		} else {
			done();
		}
	}), leo.streams.devnull(), (err) => {
		err && console.log(err);
		stream.end((err) => {
			console.log("Finished", err ? err : "");
			callback();
		});
	});
};

// On Local message trigger
if (process.send) {
	var settings;
	process.on("message", (msg) => {
		if (msg.action === "start") {
			settings = msg.cron;
			exports.handler(settings, {}, function (err, data) {
				console.log(err, data);
			});
		} else if (msg.action === "update") {
			settings.__tail.update(msg.cron);
		}
	});
} else {
	// running node oplog.js some_bot_id
	var id = process.argv[2];
	if (!id) {
		throw new Error("id required!");
	}

	getSettings(id, (err, settings) => {
		console.log(err, settings);
		if (err) {
			console.log("Error getting bot settings", id, err);
			return;
		}

		if (settings.paused) {
			console.log("Bot is paused", id)
			return;
		}

		exports.handler(settings, {}, (err) => {
			//clearInterval(updater)
			console.log(err ? `error: ${err}` : "Finished");
		});

	});
}

function getSettings(id, cb) {
	if (typeof id === "object") {
		cb(null, id);
	} else {
		console.log("getting settings");
		cron.get(id, {
			instance: 0,
			register: true
		}, cb);
	}
}

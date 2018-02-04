"use strict";

const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const combine = require("../../../lib/combine.js");
const fs = require("fs");

exports.handler = function(event, context, callback) {
	const ID = event.botId;
	let manualCheckpoint = ls.toManualCheckpoint(ID);
	ls.pipe(
		leo.read(ID, "dw.loadv2", {
			limit: 1000,
			stopTime: moment().add(240, "seconds")
		}),
		combine((table, fieldList) => {
			return ls.pipeline(ls.toCSV(fieldList, {
				trim: true,
				escape: '\\',
				nullValue: "\\N",
				delimiter: '|'
			}), fs.createWriteStream("/tmp/new_cool" + table + ".csv"));
		}),
		(err) => {
			console.log(err);
		}

	);

};
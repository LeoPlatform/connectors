"use strict";
const connect = require("./lib/connect.js");
const checksum = require("./lib/checksum.js");
const checksum2 = require("./lib/checksum2.js");

module.exports = {
	checksum: function(config) {
		return checksum(connect(config));
	},
	checksum2: function(config) {
		return checksum2(connect(config));
	}
};
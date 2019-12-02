'use strict';
const connect = require('./lib/connect.js');
const checksum = require('./lib/checksum.js');

module.exports = {
	checksum: (config) => {
		return checksum(connect(config));
	},
	connect,
};

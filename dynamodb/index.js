"use strict";
const awsFactory = require('leo-aws/factory');
const logger = require('leo-logger');
const dynamodb = awsFactory('DynamoDB');
const streamChanges = require('./lib/streamchanges');

module.exports = {
	streamChanges: function(config, opts) {

	}
};

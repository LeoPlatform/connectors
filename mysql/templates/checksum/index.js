"use strict";

const connector = require('leo-connector-mysql');
const leo = require('leo-sdk');
const secrets = leo.aws.secrets;

exports.handler = async function(event, context, callback) {
	console.log(process.env.dbsecret);
	let secret = await secrets.getSecret(process.env.dbsecret);
	console.log(secret);
	connector.checksum({
		host: secret.host,
		user: secret.username,
		port: secret.port,
		database: secret.dbname,
		password: secret.password,
		connectTimeout: 20000
	}).handler(event, context, callback);
};

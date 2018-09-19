"use strict";
const leo = require("leo-sdk");
const ls = require("leo-streams");
const config = require("leo-config");
const mysql = require("leo-connector-mysql");

exports.handler = require("leo-sdk/wrappers/cron")(async function(event, context, callback) {
	const ID = context.botId;
	let settings = Object.assign({
		source: "mysql_changes",
		destination: "my_domain_object"
	}, event);
	let connectionInfo = config.mySqlReadConnectionInfo;

	let dol = mysql.DomainObjectLoader(connectionInfo);
	let tableIdTranslations = {
		my_domain_object_table: true, // No Translation needed
		sub_object: data => `select my_domain_object_table.id from ${data.database}.sub_object where pk in (?)`,

	};
	let domainObject = new dol.DomainObject(
			c => `SELECT 
			t.pk as _domain_id,
			t.*
			FROM ${c.database}.my_domain_object_table AS t
			WHERE a.number IN (?)
			GROUP BY a.number`
		)
		.hasMany("sub_object", c => `SELECT related_table.pk as _domain_id, related_table.* from ${c.database}.related_table where pk IN (?)`);

	//Advanced Settings
	ls.pipe(
		leo.read(ID, settings.source),

		dol.translateIds(tableIdTranslations),
		dol.domainObjectTransform(domainObject),
		leo.load(ID, settings.destination),
		(err) => {
			callback(err);
		}
	)
	//End Advanced Settings
});

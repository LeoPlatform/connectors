"use strict";

require("leo-sdk/lib/logger").configure(true)
const async = require("async");

module.exports = {
	linkDimensions: function(client, table, links, callback) {
		console.log(table, links);

		let dimColumns = [];

		Object.keys(links).forEach(column => {
			let link = links[column];

			if (!link.id) {
				link = {
					table: link
				};
			}


			link = Object.assign({
				table: null,
				id: 'id'
			});
			dimColumns["d_" + column.replace(/_id$/, '')] = link;
		});


		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
			let lookup = {};
			result.forEach(r => lookup[r.column_name] = 1);


			//Let's see if we are missing any columns we should have
			let neededColumns = Object.keys(dimColumns).filter(f => !lookup[f]);
			console.log(neededColumns);


			let tasks = [];
			if (neededColumns.length) {
				tasks.push(done => {
					client.query(`alter table ${table} ` + neededColumns.map(c => `add column ${c} integer null`).join(', '), (err, result) => {
						console.log("IN HERE");
						console.log(err, result);
						callback();
					}, done);
				})
			}

			if (!("_auditdate" in lookup)) {
				tasks.push(done => client.query(`alter table ${table} add column _auditdate timestamp`, done));
				tasks.push(done => client.query(`create index ${table}_auditdate on ${table} using btree(_auditdate)`, done));
			}


			async.series(tasks, err => {

				callback(err);
			});
		});
	}
};
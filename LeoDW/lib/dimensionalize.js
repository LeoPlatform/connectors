"use strict";

// require("leo-sdk/lib/logger").configure(true)
const async = require("async");
const ls = require("leo-sdk").streams;
require("leo-sdk/lib/logger").configure(true);


module.exports = {
	linkDimensions: function(client, table, links, callback) {
		console.log(links);
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
	},
	importDimensions: function(client, stream, table, scds, callback) {
		let tasks = [];
		tasks.push(done => client.query(`drop table if exists staging_d_presenter`, done));
		tasks.push(done => client.query(`drop table if exists staging_d_presenter_changes`, done));
		tasks.push(done => client.query(`create table staging_d_presenter (like d_presenter)`, done));
		tasks.push(done => client.query(`alter table staging_d_presenter drop column d_id`, done));
		tasks.push(done => client.query(`create index staging_d_presenter_id on staging_d_presenter using hash(id)`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable("staging_d_presenter"), ls.log(), ls.devnull(), done));


		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", ["d_presenter"], (err, result) => {
			console.log(err, result);
			async.series(tasks, err => {
				if (err) {
					return callback(err);
				}

				let scd0 = Object.keys(scds).filter(f => scds[f] === 0);
				let scd2 = Object.keys(scds).filter(f => scds[f] === 2);
				let scd3 = Object.keys(scds).filter(f => scds[f] === 3);

				let scd1 = result.map(r => r.column_name).filter(f => {
					return !f.match(/^_/) && scd2.indexOf(f) === -1 && scd3.indexOf(f) === -1 && f !== 'd_id' && f !== 'id';
				});

				console.log(scd1, scd2, scd3);

				//let's figure out which SCD2 and SCD3 needs to happen
				client.query(`create table staging_d_presenter_changes as 
				select s.id, d.id is null as isNew, d._startdate, d._enddate, 
					CASE WHEN md5(${scd1.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd1.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 0 ELSE 1 END as runSCD1,
					CASE WHEN md5(${scd2.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd2.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 ELSE 1 END as runSCD2, 
					CASE WHEN md5(${scd3.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd3.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 0 ELSE 1 END as runSCD3
							FROM staging_d_presenter s
							LEFT JOIN d_presenter d on d.id = s.id and d._current`, (err, result) => {

					client.query

					console.log("stuffs", err);
					console.log(result);
					callback();

				});
			});
		});
	}
};
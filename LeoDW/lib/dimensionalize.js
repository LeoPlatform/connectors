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

				let scd0 = scds[0] || [];
				let scd2 = scds[2] || [];
				let scd3 = scds[3] || [];
				let scd6 = Object.keys(scds[6]);


				let allColumns = result.map(r => r.column_name).filter(f => {
					return !f.match(/^_/) && f !== 'd_id';
				});

				let scd1 = result.map(r => r.column_name).filter(f => {
					return !f.match(/^_/) && scd2.indexOf(f) === -1 && scd3.indexOf(f) === -1 && f !== 'd_id' && f !== 'id';
				});


				let scdSQL = [];

				if (scd1.length) {
					scdSQL.push(`CASE WHEN md5(${scd1.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd1.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 0 ELSE 1 END as runSCD1`);
				} else {
					scdSQL.push(`0 as runSCD1`);
				}
				if (scd2.length) {
					scdSQL.push(`CASE WHEN md5(${scd2.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd2.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 1 ELSE 1 END as runSCD2`);
				} else {
					scdSQL.push(`0 as runSCD2`);
				}
				if (scd3.length) {
					scdSQL.push(`CASE WHEN md5(${scd3.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd3.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 0 ELSE 1 END as runSCD3`);
				} else {
					scdSQL.push(`0 as runSCD3`);
				}
				if (scd6.length) {
					scdSQL.push(`CASE WHEN md5(${scd6.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd6.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.id is null then 0 ELSE 1 END as runSCD6`);
				} else {
					scdSQL.push(`0 as runSCD6`);
				}


				console.log(scd1, scd2, scd3, scd6);
				//let's figure out which SCDs needs to happen
				client.query(`create table staging_d_presenter_changes as 
				select s.id,
					${scdSQL.join(',\n')}
					FROM staging_d_presenter s
					LEFT JOIN d_presenter d on d.id = s.id and d._current`, (err, result) => {
					let tasks = [];
					let rowId = null;
					tasks.push(done => {
						client.query(`select max(d_id) as maxid from d_presenter`, (err, results) => {
							if (err) {
								return done(err);
							}
							rowId = results[0].maxid;
							done();
						});
					});

					tasks.push(done => client.query(`Begin Transaction`, done));

					tasks.push(done => {
						let columns = scd2.concat(scd6.map(f => "current_" + f));
						console.log(columns);
						if (!columns.length) {
							done();
						} else {
							client.query(`INSERT INTO d_presenter
								SELECT row_number() over () + ${rowId}, ${allColumns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, now() as _auditdate, now() as _startdate, null as _enddate, true as _current
								FROM staging_d_presenter_changes changes  
								JOIN staging_d_presenter staging on staging.id = changes.id
								LEFT JOIN d_presenter as prev on prev.id = changes.id and prev._current
								WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
								`, done);
						}
					});


					//This needs to be done last
					tasks.push(done => {
						//RUN SCD1 / SCD6 columns  (where we update the old records)
						let columns = scd1.map(f => `${f} = coalesce(staging.${f}, prev.${f})`).concat(scd6.map(f => `current_${f} = coalesce(staging.${f}, prev.${f})`));
						columns.push(`_enddate = case when changes.runSCD2 =1 then now() else dm._enddate END`);
						columns.push(`_current = case when changes.runSCD2 =1 then false else dm._current END`);
						columns.push(`_auditdate = now()`);
						client.query(`update d_presenter as dm
										set  ${columns.join(', ')}
										FROM staging_d_presenter_changes changes
										JOIN staging_d_presenter staging on staging.id = changes.id
										LEFT JOIN d_presenter as prev on prev.id = changes.id and prev._current
										where dm.id = changes.id and dm._startdate != now() /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having ._current*/
											and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
										`, done);
					});



					async.series(tasks, err => {
						if (!err) {
							client.query(`commit`, e => {
								callback(e || err);
							});
						} else {
							client.query(`rollback`, callback);
						}
					});
				});
			});
		});
	}
};
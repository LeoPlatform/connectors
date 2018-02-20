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
	importFact: function(client, stream, table, links, callback) {
		let tasks = [];
		tasks.push(done => client.query(`drop table if exists staging_${table}`, done));
		tasks.push(done => client.query(`drop table if exists staging_${table}_changes`, done));
		tasks.push(done => client.query(`create table staging_${table} (like ${table})`, done));
		tasks.push(done => client.query(`create index staging_${table}_id on staging_${table} using hash(id)`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(`staging_${table}`), ls.log(), ls.devnull(), done));

		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
			let columns = result.map(f => f.column_name).filter(f => !f.match(/^_/));

			console.log(links);
			async.series(tasks, err => {
				if (err) {
					return callback(err);
				}

				let tasks = [];
				//The following code relies on the fact that now() will return the same time during all transaction events
				tasks.push(done => client.query(`Begin Transaction`, done));

				tasks.push(done => {
					client.query(`Update ${table} dm
								SET  ${columns.map(f=>`${f} = coalesce(staging.${f}, prev.${f})`)}, _auditdate = now()
								FROM staging_${table} staging
								JOIN ${table} as prev on prev.id = staging.id
								where dm.id = staging.id 
							`, done);
				});


				//Now insert any we were missing
				tasks.push(done => {
					client.query(`INSERT INTO ${table} (${columns.join(',')},_auditdate)
								SELECT ${columns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, now() as _auditdate
								FROM staging_${table} staging
								LEFT JOIN ${table} as prev on prev.id = staging.id
								WHERE prev.id is null	
							`, done);
				});



				//Now we want to link any dimensions
				tasks.push(done => {
					let joinTables = Object.keys(links).map(f => {
						return `LEFT JOIN ${links[f]} ${f}_join_table on ${f}_join_table.id = t.${f} and t.created >= ${f}_join_table._startdate and (t.created <= ${f}_join_table._enddate or ${f}_join_table._current)`;
					});

					client.query(`Update ${table} dm
								SET  ${Object.keys(links).map(f=>`d_${f.replace(/_id$/, '')} = ${f}_join_table.d_id`)}
								FROM ${table} t
								${joinTables.join("\n")}
								where dm.id = t.id 
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

	},
	importDimension: function(client, stream, table, scds, callback) {
		let tasks = [];
		tasks.push(done => client.query(`drop table if exists staging_${table}`, done));
		tasks.push(done => client.query(`drop table if exists staging_${table}_changes`, done));
		tasks.push(done => client.query(`create table staging_${table} (like ${table})`, done));
		tasks.push(done => client.query(`alter table staging_${table} drop column d_id`, done));
		tasks.push(done => client.query(`create index staging_${table}_id on staging_${table} using hash(id)`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(`staging_${table}`), ls.log(), ls.devnull(), done));

		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
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


				//let's figure out which SCDs needs to happen
				client.query(`create table staging_${table}_changes as 
				select s.id,
					${scdSQL.join(',\n')}
					FROM staging_${table} s
					LEFT JOIN ${table} d on d.id = s.id and d._current`, (err, result) => {
					let tasks = [];
					let rowId = null;
					tasks.push(done => {
						client.query(`select max(d_id) as maxid from ${table}`, (err, results) => {
							if (err) {
								return done(err);
							}
							rowId = results[0].maxid;
							done();
						});
					});


					//The following code relies on the fact that now() will return the same time during all transaction events
					tasks.push(done => client.query(`Begin Transaction`, done));

					tasks.push(done => {
						let columns = scd2.concat(scd6.map(f => "current_" + f));
						console.log(columns);
						if (!columns.length) {
							done();
						} else {
							client.query(`INSERT INTO ${table}
								SELECT row_number() over () + ${rowId}, ${allColumns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, now() as _auditdate, now() as _startdate, null as _enddate, true as _current
								FROM staging_${table}_changes changes  
								JOIN staging_${table} staging on staging.id = changes.id
								LEFT JOIN ${table} as prev on prev.id = changes.id and prev._current
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
						client.query(`update ${table} as dm
										set  ${columns.join(', ')}
										FROM staging_${table}_changes changes
										JOIN staging_${table} staging on staging.id = changes.id
										LEFT JOIN ${table} as prev on prev.id = changes.id and prev._current
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
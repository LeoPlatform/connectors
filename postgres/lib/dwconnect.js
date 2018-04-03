"use strict";
const postgres = require("./connect.js");
const async = require("async");
const ls = require("leo-sdk").streams;


module.exports = function(config) {
	let client = postgres(config);

	client.importFact = function(stream, table, ids, callback) {
		if (!Array.isArray(ids)) {
			ids = [ids];
		}

		let tasks = [];
		// tasks.push(done => client.query(`alter table ${table} add primary key (${ids.join(',')})`, done));

		tasks.push(done => client.query(`drop table if exists staging_${table}`, done));
		tasks.push(done => client.query(`drop table if exists staging_${table}_changes`, done));
		tasks.push(done => client.query(`create table staging_${table} (like ${table})`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(`staging_${table}`), done));

		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
			let columns = result.map(f => f.column_name).filter(f => !f.match(/^_/));

			client.connect().then(client => {
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
								JOIN ${table} as prev on ${ids.map(id=>`prev.${id} = staging.${id}`).join(' and ')}
								where ${ids.map(id=>`dm.${id} = staging.${id}`).join(' and ')}
							`, done);
					});


					//Now insert any we were missing
					tasks.push(done => {
						client.query(`INSERT INTO ${table} (${columns.join(',')},_auditdate)
								SELECT ${columns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, now() as _auditdate
								FROM staging_${table} staging
								LEFT JOIN ${table} as prev on ${ids.map(id=>`prev.${id} = staging.${id}`).join(' and ')}
								WHERE prev.${ids[0]} is null	
							`, done);
					});
					tasks.push(done => client.query(`drop table staging_${table}`, done));

					async.series(tasks, err => {
						if (!err) {
							client.query(`commit`, e => {
								client.release();
								callback(e || err);
							});
						} else {
							client.query(`rollback`, (e, d) => {
								client.release();
								callback(e, d);
							});
						}
					});
				});
			}).catch(callback);
		});
	};

	client.importDimension = function(stream, table, sk, nk, scds, callback) {
		if (!Array.isArray(nk)) {
			nk = [nk];
		}

		let tasks = [];
		tasks.push(done => client.query(`drop table if exists staging_${table}`, done));
		tasks.push(done => client.query(`drop table if exists staging_${table}_changes`, done));
		tasks.push(done => client.query(`create table staging_${table} (like ${table})`, done));
		tasks.push(done => client.query(`alter table staging_${table} drop column ${sk}`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(`staging_${table}`), done));

		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {

			client.connect().then(client => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let scd0 = scds[0] || [];
					let scd2 = scds[2] || [];
					let scd3 = scds[3] || [];
					let scd6 = Object.keys(scds[6] || {});


					let allColumns = result.map(r => r.column_name).filter(f => {
						return !f.match(/^_/) && f !== sk;
					});

					let scd1 = result.map(r => r.column_name).filter(f => {
						return !f.match(/^_/) && scd2.indexOf(f) === -1 && scd3.indexOf(f) === -1 && f !== sk && nk.indexOf(f) === -1;
					});


					let scdSQL = [];

					if (scd1.length) {
						scdSQL.push(`CASE WHEN md5(${scd1.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd1.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD1`);
					} else {
						scdSQL.push(`0 as runSCD1`);
					}
					if (scd2.length) {
						scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 WHEN md5(${scd2.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd2.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 ELSE 1 END as runSCD2`);
					} else {
						scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 ELSE 0 END as runSCD2`);
					}
					if (scd3.length) {
						scdSQL.push(`CASE WHEN md5(${scd3.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd3.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD3`);
					} else {
						scdSQL.push(`0 as runSCD3`);
					}
					if (scd6.length) {
						scdSQL.push(`CASE WHEN md5(${scd6.map(f => "md5(coalesce(s."+f+"::text,''))" ).join(' || ')}) = md5(${scd6.map(f => "md5(coalesce(d."+f+"::text,''))" ).join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD6`);
					} else {
						scdSQL.push(`0 as runSCD6`);
					}

					//let's figure out which SCDs needs to happen
					client.query(`create table staging_${table}_changes as 
				select ${nk.map(id=>`s.${id}`).join(', ')}, d.${nk[0]} is null as isNew,
					${scdSQL.join(',\n')}
					FROM staging_${table} s
					LEFT JOIN ${table} d on ${nk.map(id=>`d.${id} = s.${id}`).join(' and ')} and d._current`, (err, result) => {
						if (err) {
							console.log(err);
							process.exit();
						}
						let tasks = [];
						let rowId = null;
						tasks.push(done => {
							client.query(`select max(${sk}) as maxid from ${table}`, (err, results) => {
								if (err) {
									return done(err);
								}
								rowId = results[0].maxid || 10000;
								done();
							});
						});


						//The following code relies on the fact that now() will return the same time during all transaction events
						tasks.push(done => client.query(`Begin Transaction`, done));

						tasks.push(done => {
							let fields = [sk].concat(allColumns).concat(['_auditdate', '_startdate', '_enddate', '_current']);
							client.query(`INSERT INTO ${table} (${fields.join(',')})
								SELECT row_number() over () + ${rowId}, ${allColumns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, now() as _auditdate, case when changes.isNew then '1900-01-01 00:00:00' else now() END as _startdate, '9999-01-01 00:00:00' as _enddate, true as _current
								FROM staging_${table}_changes changes  
								JOIN staging_${table} staging on ${nk.map(id=>`staging.${id} = changes.${id}`).join(' and ')}
								LEFT JOIN ${table} as prev on ${nk.map(id=>`prev.${id} = changes.${id}`).join(' and ')} and prev._current
								WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
								`, done);
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
										JOIN staging_${table} staging on ${nk.map(id=>`staging.${id} = changes.${id}`).join(' and ')}
										LEFT JOIN ${table} as prev on ${nk.map(id=>`prev.${id} = changes.${id}`).join(' and ')} and prev._current
										where ${nk.map(id=>`dm.${id} = changes.${id}`).join(' and ')} and dm._startdate != now() and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having ._current*/
											and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
										`, done);
						});

						tasks.push(done => client.query(`drop table staging_${table}_changes`, done));
						tasks.push(done => client.query(`drop table staging_${table}`, done));
						async.series(tasks, err => {
							if (!err) {
								client.query(`commit`, e => {
									client.release();
									callback(e || err);
								});
							} else {
								client.query(`rollback`, (e, d) => {
									client.release();
									callback(e, d);
								});
							}
						});
					});
				});
			}).catch(callback);
		});
	};

	client.linkDimensions = function(table, links, nk, callback) {
		client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
			let tasks = [];
			let sets = [];
			tasks.push(done => client.query(`analyze ${table}`, done));
			tasks.push(done => {
				let joinTables = links.map(link => {
					if (link.table == "datetime" || link.table == "dim_datetime") {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else if (link.table == "d_date" || link.table == "dim_date") {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
					} else if (link.table == "d_time" || link.table == "dim_time") {
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else {
						sets.push(`${link.destination} = coalesce(${link.source}_join_table.${link.sk}, 1)`);
						return `LEFT JOIN ${link.table} ${link.source}_join_table 
							on ${link.source}_join_table.${link.on} = t.${link.source} 
								and t.${link.link_date} >= ${link.source}_join_table._startdate 
								and (t.${link.link_date} <= ${link.source}_join_table._enddate or ${link.source}_join_table._current)
					`;
					}
				});
				client.query(`Update ${table} dm
						SET  ${sets.join(', ')}
						FROM ${table} t
						${joinTables.join("\n")}
						where ${nk.map(id=>`dm.${id} = t.${id}`).join(' and ')}
					`, done);
			});
			async.series(tasks, err => {
				callback(err);
			});
		});
	};

	client.changeTableStructure = function(structures, callback) {
		let tasks = [];
		Object.keys(structures).forEach(table => {
			tasks.push(done => {
				client.describeTable(table, (err, fields) => {
					if (err) return done(err);
					if (!fields.length) {
						client.createTable(table, structures[table], done);
					} else {
						let fieldLookup = fields.reduce((acc, field) => {
							acc[field.column_name] = field;
							return acc;
						}, {});
						let missingFields = {};
						Object.keys(structures[table].structure).forEach(f => {
							if (!(f in fieldLookup)) {
								missingFields[f] = structures[table].structure[f];
							}
						});
						if (Object.keys(missingFields).length) {
							client.updateTable(table, missingFields, done);
						} else {
							done();
						}
					}
				});
			});
		});
		async.parallelLimit(tasks, 20, callback);
	};

	client.createTable = function(table, definition, callback) {
		let fields = [];

		let ids = [];
		Object.keys(definition.structure).forEach(f => {
			let field = definition.structure[f];
			if (field == "sk") {
				field = {
					type: 'integer primary key'
				};
			} else if (typeof field == "string") {
				field = {
					type: field
				};
			}

			if (field == "nk" || field.nk) {
				ids.push(f);
			}

			if (field.dimension == "datetime" || field.dimension == "dim_datetime") {
				fields.push(`d_${f.replace(/_id$/,'')}_date integer`);
				fields.push(`d_${f.replace(/_id$/,'')}_time integer`);
			} else if (field.dimension == "date" || field.dimension == "dim_date") {
				fields.push(`d_${f.replace(/_id$/,'')}_date integer`);
			} else if (field.dimension == "time" || field.dimension == "dim_time") {
				fields.push(`d_${f.replace(/_id$/,'')}_time integer`);
			} else if (field.dimension) {
				fields.push(`d_${f.replace(/_id$/,'')} integer`);
			}
			fields.push(`"${f}" ${field.type}`);
		});

		let sql = `create table ${table} (
				${fields.join(',\n')}
			)`;

		let tasks = [];
		tasks.push(done => client.query(sql, done));
		if (definition.isDimension) {
			tasks.push(done => client.query(`alter table ${table} add column _auditdate timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column _startdate timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column _enddate timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column _current boolean`, done));
			tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.concat("_current").join(',')})`, done));
			tasks.push(done => client.query(`create index ${table}_bk2 on ${table} using btree(${ids.concat("_startdate").join(',')})`, done));
			tasks.push(done => client.query(`create index ${table}_auditdate on ${table} using btree(_auditdate)`, done));
		} else {
			tasks.push(done => client.query(`alter table ${table} add column _auditdate timestamp`, done));
			tasks.push(done => client.query(`create index ${table}_auditdate on ${table} using btree(_auditdate)`, done));
			tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.join(',')})`, done));
		}


		async.series(tasks, callback);
	};
	client.updateTable = function(table, definition, callback) {
		let fields = [];
		Object.keys(definition).forEach(f => {
			let field = definition[f];
			if (field == "sk") {
				field = {
					type: 'integer primary key'
				};
			} else if (typeof field == "string") {
				field = {
					type: field
				};
			}

			if (field.dimension == "datetime" || field.dimension == "dim_datetime") {
				fields.push(`d_${f.replace(/_id$/,'')}_date integer`);
				fields.push(`d_${f.replace(/_id$/,'')}_time integer`);
			} else if (field.dimension == "date" || field.dimension == "dim_date") {
				fields.push(`d_${f.replace(/_id$/,'')}_date integer`);
			} else if (field.dimension == "time" || field.dimension == "dim_date") {
				fields.push(`d_${f.replace(/_id$/,'')}_time integer`);
			} else if (field.dimension) {
				fields.push(`d_${f.replace(/_id$/,'')} integer`);
			}
			fields.push(`"${f}" ${field.type}`);
		});
		let sql = `alter table  ${table} 
				add column ${fields.join(',\n add column ')}
			`;
		client.query(sql, callback);
	};

	return client;
};

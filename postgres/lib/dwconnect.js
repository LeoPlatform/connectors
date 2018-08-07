"use strict";
const postgres = require("./connect.js");
const async = require("async");
const ls = require("leo-sdk").streams;

module.exports = function(config, columnConfig) {
	let client = postgres(config);
	let dwClient = client;
	columnConfig = Object.assign({
		_auditdate: '_auditdate',
		_startdate: '_startdate',
		_enddate: '_enddate',
		_current: '_current',
		dimColumnTransform: (column) => {
			return `d_${column.replace(/_id$/,'')}`;
		},
		useSurrogateDateKeys: true,
		stageSchema: 'public'
	}, columnConfig || {});

	client.getDimensionColumn = columnConfig.dimColumnTransform;

	client.importFact = function(stream, table, ids, callback) {
		const stagingTbl = `${columnConfig.stageSchema}.staging_${table}`;
		const publicTbl = `public.${table}`;
		if (!Array.isArray(ids)) {
			ids = [ids];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		schema[stagingTbl] = schema[publicTbl];

		let tasks = [];
		// tasks.push(done => client.query(`alter table ${table} add primary key (${ids.join(',')})`, done));

		tasks.push(done => client.query(`drop table if exists ${stagingTbl}`, done));
		tasks.push(done => client.query(`drop table if exists ${stagingTbl}_changes`, done));
		tasks.push(done => client.query(`create table ${stagingTbl} (like ${publicTbl})`, done));
		tasks.push(done => client.query(`create index ${stagingTbl}_id on ${stagingTbl} (${ids.join(', ')})`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(stagingTbl), done));
		tasks.push(done => client.query(`analyze ${stagingTbl}`, done));

		client.describeTable(table, (err, result) => {
			let columns = result.filter(f => !f.column_name.match(/^_/)).map(f => `"${f.column_name}"`);

			client.connect().then(client => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let tasks = [];
					let totalRecords = 0;
					//The following code relies on the fact that now() will return the same time during all transaction events
					tasks.push(done => client.query(`Begin Transaction`, done));
					tasks.push(done => {
						client.query(`select 1 as total from ${table} limit 1`, (err, results) => {
							if (err) {
								return done(err);
							}
							totalRecords = results.length;
							done();
						});
					});
					tasks.push(done => {
						client.query(`Update ${publicTbl} prev
								SET  ${columns.map(f=>`${f} = coalesce(staging.${f}, prev.${f})`)}, ${columnConfig._auditdate} = ${dwClient.auditdate}
								FROM ${stagingTbl} staging
								where ${ids.map(id=>`prev.${id} = staging.${id}`).join(' and ')}
							`, done);
					});


					//Now insert any we were missing
					tasks.push(done => {
						client.query(`INSERT INTO ${publicTbl} (${columns.join(',')},${columnConfig._auditdate})
								SELECT ${columns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, ${dwClient.auditdate} as ${columnConfig._auditdate}
								FROM ${stagingTbl} staging
								LEFT JOIN ${publicTbl} as prev on ${ids.map(id=>`prev.${id} = staging.${id}`).join(' and ')}
								WHERE prev.${ids[0]} is null	
							`, done);
					});
					// tasks.push(done => client.query(`drop table staging_${table}`, done));

					async.series(tasks, err => {
						if (!err) {
							client.query(`commit`, e => {
								client.release();
								callback(e || err, {
									count: totalRecords
								});
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
		const stagingTbl = `${columnConfig.stageSchema}.staging_${table}`;
		const publicTbl = `public.${table}`;
		if (!Array.isArray(nk)) {
			nk = [nk];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		if (typeof schema[publicTbl] === 'undefined') {
			throw new Error(`${publicTbl} not found in schema`);
		}
		schema[stagingTbl] = schema[publicTbl].filter(c => c.column_name != sk);

		let tasks = [];
		tasks.push(done => client.query(`drop table if exists ${stagingTbl}`, done));
		tasks.push(done => client.query(`drop table if exists ${stagingTbl}_changes`, done));
		tasks.push(done => client.query(`create table ${stagingTbl} (like ${publicTbl})`, done));
		tasks.push(done => client.query(`create index ${stagingTbl}_id on ${stagingTbl} (${nk.join(', ')})`, done));
		tasks.push(done => client.query(`alter table ${stagingTbl} drop column ${sk}`, done));
		tasks.push(done => ls.pipe(stream, client.streamToTable(stagingTbl), done));
		tasks.push(done => client.query(`analyze ${stagingTbl}`, done));

		client.describeTable(table, (err, result) => {
			client.connect().then(client => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let scd0 = scds[0] || [];
					let scd2 = scds[2] || [];
					let scd3 = scds[3] || [];
					let scd6 = Object.keys(scds[6] || {});

					let ignoreColumns = [columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current];
					let allColumns = result.filter(f => {
						return ignoreColumns.indexOf(f.column_name) === -1 && f.column_name !== sk;
					}).map(r => `"${r.column_name}"`);

					let scd1 = result.map(r => r.column_name).filter(f => {
						return ignoreColumns.indexOf(f) === -1 && scd2.indexOf(f) === -1 && scd3.indexOf(f) === -1 && f !== sk && nk.indexOf(f) === -1;
					});


					let scdSQL = [];

					//if (!scd2.length && !scd3.length && !scd6.length) {
					//	scdSQL.push(`1 as runSCD1`);
					//} else 
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
					client.query(`create table ${stagingTbl}_changes as 
				select ${nk.map(id=>`s.${id}`).join(', ')}, d.${nk[0]} is null as isNew,
					${scdSQL.join(',\n')}
					FROM ${stagingTbl} s
					LEFT JOIN ${table} d on ${nk.map(id=>`d.${id} = s.${id}`).join(' and ')} and d.${columnConfig._current}`, (err, result) => {
						if (err) {
							console.log(err);
							process.exit();
						}
						let tasks = [];
						let rowId = null;
						let totalRecords = 0;
						tasks.push(done => client.query(`analyze staging_${table}_changes`, done));
						tasks.push(done => {
							client.query(`select max(${sk}) as maxid from ${table}`, (err, results) => {
								if (err) {
									return done(err);
								}
								rowId = results[0].maxid || 10000;
								totalRecords = (rowId - 10000);
								done();
							});
						});


						//The following code relies on the fact that now() will return the same time during all transaction events
						tasks.push(done => client.query(`Begin Transaction`, done));

						tasks.push(done => {
							let fields = [sk].concat(allColumns).concat([columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current]);
							client.query(`INSERT INTO ${publicTbl} (${fields.join(',')})
								SELECT row_number() over () + ${rowId}, ${allColumns.map(f=>`coalesce(staging.${f}, prev.${f})`)}, ${dwClient.auditdate} as ${columnConfig._auditdate}, case when changes.isNew then '1900-01-01 00:00:00' else now() END as ${columnConfig._startdate}, '9999-01-01 00:00:00' as ${columnConfig._enddate}, true as ${columnConfig._current}
								FROM ${stagingTbl}_changes changes  
								JOIN ${stagingTbl} staging on ${nk.map(id=>`staging.${id} = changes.${id}`).join(' and ')}
								LEFT JOIN ${publicTbl} as prev on ${nk.map(id=>`prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._current}
								WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
								`, done);
						});

						//This needs to be done last
						tasks.push(done => {
							//RUN SCD1 / SCD6 columns  (where we update the old records)
							let columns = scd1.map(f => `"${f}" = coalesce(staging."${f}", prev."${f}")`).concat(scd6.map(f => `"current_${f}" = coalesce(staging."${f}", prev."${f}")`));
							columns.push(`"${columnConfig._enddate}" = case when changes.runSCD2 =1 then now() else prev."${columnConfig._enddate}" END`);
							columns.push(`"${columnConfig._current}" = case when changes.runSCD2 =1 then false else prev."${columnConfig._current}" END`);
							columns.push(`"${columnConfig._auditdate}" = ${dwClient.auditdate}`);
							client.query(`update ${publicTbl} as prev
										set  ${columns.join(', ')}
										FROM ${stagingTbl}_changes changes
										JOIN ${stagingTbl} staging on ${nk.map(id=>`staging.${id} = changes.${id}`).join(' and ')}
										LEFT JOIN ${publicTbl} as prev on ${nk.map(id=>`prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._current}
										where ${nk.map(id=>`dm.${id} = changes.${id}`).join(' and ')} and dm.${columnConfig._startdate} != now() and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having .${columnConfig._current}*/
											and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
										`, done);
						});

						tasks.push(done => client.query(`drop table ${stagingTbl}_changes`, done));
						tasks.push(done => client.query(`drop table ${stagingTbl}`, done));
						async.series(tasks, err => {
							if (!err) {
								client.query(`commit`, e => {
									client.release();
									callback(e || err, {
										count: totalRecords
									});
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

	client.insertMissingDimensions = function(usedTables, tableConfig, tableSks, tableNks, callback) {
		let unions = {};
		let isDate = {
			d_datetime: true,
			datetime: true,
			dim_datetime: true,
			d_date: true,
			date: true,
			dim_date: true,
			d_time: true,
			time: true,
			dim_time: true
		};
		Object.keys(usedTables).map(table => {
			Object.keys(tableConfig[table].structure).map(column => {
				let field = tableConfig[table].structure[column];
				if (field.dimension && !isDate[field.dimension]) {
					if (!(unions[field.dimension])) {
						unions[field.dimension] = [];
					}
					if (typeof tableNks[field.dimension] === 'undefined') {
						throw new Error(`${field.dimension} not found in tableNks`);
					}
					let dimTableNk = tableNks[field.dimension][0];
					unions[field.dimension].push(`select ${table}.${column} as id from ${table} left join ${field.dimension} on ${field.dimension}.${dimTableNk} = ${table}.${column} where ${field.dimension}.${dimTableNk} is null and ${table}.${columnConfig._auditdate} = ${dwClient.auditdate}`);
				}
			});
		});
		let missingDimTasks = Object.keys(unions).map(table => {
			let sk = tableSks[table];
			let nk = tableNks[table][0];
			return (callback) => {
				let done = (err, data) => {
					trx && trx.release();
					callback(err, data);
				};
				let trx;
				client.connect().then(transaction => {
					trx = transaction;
					transaction.query(`select max(${sk}) as maxid from ${table}`, (err, results) => {
						if (err) {
							return done(err);
						}
						let rowId = results[0].maxid || 10000;
						let _auditdate = dwClient.auditdate; //results[0]._auditdate ? `'${results[0]._auditdate.replace(/^\d* */,"")}'` : "now()";
						let unionQuery = unions[table].join("\nUNION\n");
						transaction.query(`insert into ${table} (${sk}, ${nk}, ${columnConfig._auditdate}, ${columnConfig._startdate}, ${columnConfig._enddate}, ${columnConfig._current}) select row_number() over () + ${rowId}, sub.id, max(${_auditdate})::timestamp, '1900-01-01 00:00:00', '9999-01-01 00:00:00', true from (${unionQuery}) as sub where sub.id is not null group by sub.id`, (err) => {
							done(err);
						});
					});
				}).catch(done);
			};
		});
		async.parallelLimit(missingDimTasks, 10, (missingDimError) => {
			console.log(`Missing Dimensions ${!missingDimError && "Inserted"} ----------------------------`, missingDimError || "");
			callback(missingDimError);
		});

	};

	client.linkDimensions = function(table, links, nk, callback, tableStatus) {
		client.describeTable(table, (err, result) => {
			let tasks = [];
			let sets = [];

			// Only run analyze on the table if this is the first load
			if (tableStatus === "First Load") {
				tasks.push(done => client.query(`analyze ${table}`, done));
			}
			tasks.push(done => {
				let joinTables = links.map(link => {
					if (link.table == "d_datetime" || link.table == "datetime" || link.table == "dim_datetime") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
							sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
						}
					} else if (link.table == "d_date" || link.table == "date" || link.table == "dim_date") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
						}
					} else if (link.table == "d_time" || link.table == "time" || link.table == "dim_time") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
						}
					} else {
						sets.push(`${link.destination} = coalesce(${link.source}_join_table.${link.sk}, 1)`);
						return `LEFT JOIN ${link.table} ${link.source}_join_table 
							on ${link.source}_join_table.${link.on} = t.${link.source} 
								and t.${link.link_date} >= ${link.source}_join_table.${columnConfig._startdate}
								and (t.${link.link_date} <= ${link.source}_join_table.${columnConfig._enddate} or ${link.source}_join_table.${columnConfig._current})
					`;
					}
				});

				if (sets.length) {
					client.query(`Update ${table} dm
                        SET  ${sets.join(', ')}
                        FROM ${table} t
                        ${joinTables.join("\n")}
                        where ${nk.map(id=>`dm.${id} = t.${id}`).join(' and ')}
                            AND dm.${columnConfig._auditdate} = ${dwClient.auditdate} AND t.${columnConfig._auditdate} = ${dwClient.auditdate}
                    `, done);
				} else {
					done();
				}
			});
			async.series(tasks, err => {
				callback(err);
			});
		});
	};

	client.changeTableStructure = function(structures, callback) {
		let tasks = [];
		let tableResults = {};

		Object.keys(structures).forEach(table => {
			tableResults[table] = "Unmodified";
			tasks.push(done => {
				client.describeTable(table, (err, fields) => {
					if (err) return done(err);
					if (!fields || !fields.length) {
						tableResults[table] = "Created";
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
							tableResults[table] = "Modified";
							client.updateTable(table, missingFields, done);
						} else {
							done();
						}
					}
				});
			});
		});
		async.parallelLimit(tasks, 20, (err) => {
			if (err) return callback(err, tableResults);
			//Update client schema cache with new/updated tables
			client.describeTables((err) => {
				callback(err, tableResults);
			});
		});
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

			if (field.dimension == "d_datetime" || field.dimension == "datetime" || field.dimension == "dim_datetime") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_date integer`);
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_time integer`);
				}
			} else if (field.dimension == "d_date" || field.dimension == "date" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_date integer`);
				}
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_time") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_time integer`);
				}
			} else if (field.dimension) {
				fields.push(`${columnConfig.dimColumnTransform(f, field)} integer`);
			}
			fields.push(`"${f}" ${field.type}`);
		});

		let sql = `create table ${table} (
				${fields.join(',\n')}
			)`;


		/*
			@todo if dimension, add empty row
		*/
		let tasks = [];
		tasks.push(done => client.query(sql, done));
		if (definition.isDimension) {
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._auditdate} timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._startdate} timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._enddate} timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._current} boolean`, done));

			// redshift doesn't support create index
			if (config.version != "redshift") {
				tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.concat(columnConfig._current).join(',')})`, done));
				tasks.push(done => client.query(`create index ${table}_bk2 on ${table} using btree(${ids.concat(columnConfig._startdate).join(',')})`, done));
				tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${table} using btree(${columnConfig._auditdate})`, done));
			}
		} else {
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._auditdate} timestamp`, done));

			// redshift doesn't support create index
			if (config.version != "redshift") {
				tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${table} using btree(${columnConfig._auditdate})`, done));
				tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.join(',')})`, done));
			}
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

			if (field.dimension == "d_datetime" || field.dimension == "datetime" || field.dimension == "dim_datetime") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_date integer`);
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_time integer`);
				}
			} else if (field.dimension == "d_date" || field.dimension == "date" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_date integer`);
				}
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(f, field)}_time integer`);
				}
			} else if (field.dimension) {
				fields.push(`${columnConfig.dimColumnTransform(f, field)} integer`);
			}
			fields.push(`"${f}" ${field.type}`);
		});
		let sqls = [`alter table  ${table} 
				add column ${fields.join(',\n add column ')}
			`];

		// redshift doesn't support multi 'add column' in one query
		if (config.version == "redshift") {
			sqls = fields.map(f => `alter table  ${table} add column ${f}`);
		}

		async.eachSeries(sqls, function(sql, done) {
			client.query(sql, done);
		}, callback);
	};

	client.findAuditDate = function(table, callback) {
		client.query(`select to_char(max(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as max FROM ${client.escapeId(table)}`, (err, auditdate) => {
			if (err) {
				callback(err);
			} else {
				let audit = auditdate && auditdate[0].max;
				let auditdateCompare = audit != null ? `${columnConfig._auditdate} >= ${client.escapeValue(audit)}` : `${columnConfig._auditdate} is null`;
				client.query(`select count(*) as count FROM ${client.escapeId(table)} where ${auditdateCompare}`, (err, count) => {
					callback(err, {
						auditdate: audit,
						count: count && count[0].count
					});
				});
			}
		});
	};

	client.exportChanges = function(table, fields, remoteAuditdate, opts, callback) {
		let auditdateCompare = remoteAuditdate.auditdate != null ? `${columnConfig._auditdate} >= ${client.escapeValue(remoteAuditdate.auditdate)}` : `${columnConfig._auditdate} is null`;
		client.query(`select count(*) as count FROM ${client.escapeId(table)} WHERE ${auditdateCompare}`, (err, result) => {
			let where = "";

			let mysqlAuditDate = parseInt(result[0].count);

			if (remoteAuditdate.auditdate && mysqlAuditDate <= remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} > ${client.escapeValue(remoteAuditdate.auditdate)}`;
			} else if (remoteAuditdate.auditdate && mysqlAuditDate > remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} >= ${client.escapeValue(remoteAuditdate.auditdate)}`;
			}
			client.query(`select to_char(min(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as oldest, count(*) as count
        FROM ${client.escapeId(table)}
        ${where}
        `, (err, result) => {

				if (result[0].count) {
					let totalCount = parseInt(result[0].count);
					let needs = {
						[columnConfig._auditdate]: true,
						[columnConfig._current]: opts.isDimension,
						[columnConfig._startdate]: opts.isDimension,
						[columnConfig._enddate]: opts.isDimension

					};
					var f = fields.map(f => {
						needs[f] = false;
						return `"${f}"`;
					});
					Object.keys(needs).map(key => {
						if (needs[key]) {
							f.push(`"${key}"`);
						}
					});

					if (config.version == "redshift") {
						let fileBase = `s3://${opts.bucket}${opts.file}/${table}`;
						let query = `UNLOAD ('select ${f} from ${client.escapeId(table)} ${where}') to '${fileBase}' MANIFEST OVERWRITE ESCAPE iam_role '${opts.role}';`;
						client.query(query, (err) => {
							callback(err, fileBase + ".manifest", totalCount, result[0].oldest);
						});
					} else {
						let file = `${opts.file}/${table}.csv`;
						ls.pipe(client.streamFromTable(table, {
							columns: f,
							header: false,
							delimeter: "|",
							where: where
						}), ls.toS3(opts.bucket, file), (err) => {
							err && console.log("Stream From table Error:", err);
							callback(err, "s3://" + opts.bucket + "/" + file, totalCount, result[0].oldest);
						});
					}
				} else {
					callback(null, null, 0, null);
				}
			});
		});
	};

	client.importChanges = function(file, table, fields, opts, callback) {
		if (typeof opts === "function") {
			callback = opts;
			opts = {};
		}
		opts = Object.assign({
			role: null
		}, opts);
		var tableName = table.identifier;
		var tasks = [];
		let loadCount = 0;
		let stageTable = `${columnConfig.stageSchema}.staging_${table}`;
		tasks.push((done) => {
			client.query(`drop table if exists ${stageTable}`, done);
		});
		tasks.push((done) => {
			client.query(`create /*temporary*/ table ${stageTable} (like ${tableName})`, done);
		});
		tasks.push((done) => {
			let needs = {
				[columnConfig._auditdate]: true,
				[columnConfig._current]: opts.isDimension,
				[columnConfig._startdate]: opts.isDimension,
				[columnConfig._enddate]: opts.isDimension

			};
			var f = fields.map(f => {
				needs[f] = false;
				return `"${f}"`;
			});
			Object.keys(needs).map(key => {
				if (needs[key]) {
					f.push(`"${key}"`);
				}
			});
			if (config.version == "redshift") {
				let manifest = "";
				if (file.match(/\.manifest$/)) {
					manifest = "MANIFEST";
				}
				client.query(`copy ${stageTable} (${f})
          from '${file}' ${manifest} ${opts.role?`credentials 'aws_iam_role=${opts.role}'`: ""}
		  NULL AS '\\\\N' format csv DELIMITER '|' ACCEPTINVCHARS TRUNCATECOLUMNS ACCEPTANYDATE TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' COMPUPDATE OFF`, done);
			} else {
				done("Postgres importChanges Not Implemented Yet");
			}
		});
		if (table.isDimension) {
			tasks.push((done) => {
				client.query(`delete from ${tableName} using ${stageTable} where ${stageTable}.${table.sk}=${tableName}.${table.sk}`, done);
			});
		} else {
			tasks.push((done) => {
				let ids = table.nks.map(nk => `${stageTable}.${nk}=${tableName}.${nk}`).join(' and ');
				client.query(`delete from ${tableName} using ${stageTable} where ${ids}`, done);
			});
		}
		tasks.push(function(done) {
			client.query(`insert into ${tableName} select * from ${stageTable}`, done);
		});
		tasks.push(function(done) {
			client.query(`select count(*) from ${stageTable}`, (err, result) => {
				loadCount = result && parseInt(result[0].count);
				done(err);
			});
		});
		tasks.push(function(done) {
			client.query(`drop table if exists ${stageTable}`, done);
		});
		async.series(tasks, (err) => {
			callback(err, loadCount);
		});
	};

	return client;
};

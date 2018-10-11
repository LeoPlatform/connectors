"use strict";
const mysql = require("./connect.js");
const async = require("async");
const ls = require("leo-sdk").streams;

module.exports = function(config, columnConfig) {
	let client = mysql(config);
	let dwClient = client;
	columnConfig = Object.assign({
		_deleted: '_deleted',
		_auditdate: '_auditdate',
		_startdate: '_startdate',
		_enddate: '_enddate',
		_current: '_current',
		dimColumnTransform: (column, field) => {
			field = field || {};
			let dimCol = field[`dim_column${column.replace(field.id, "")}`];
			if (dimCol) {
				return dimCol;
			}
			return field.dim_column ? field.dim_column : `d_${column.replace(/_id$/,'').replace(/^d_/,'')}`
		},
		useSurrogateDateKeys: true,
		stageSchema: config.database || 'datawarehouse'
	}, columnConfig || {});

	client.getDimensionColumn = columnConfig.dimColumnTransform;
	client.columnConfig = columnConfig;

	function deletesSetup(qualifiedTable, schema, field, value, where = "") {
		let colLookup = {};
		schema.map(col => {
			colLookup[col.column_name] = true;
		});
		let toDelete = {};
		let toDeleteCount = 0;
		if (where) {
			where = `and ${where}`;
		}

		function tryFlushDelete(done, force = false) {
			if (force || toDeleteCount >= 1000) {
				let deleteTasks = Object.keys(toDelete).map(col => {
					return deleteDone => client.query(`update ${qualifiedTable} set ?? = ? where ? in (?) ${where}`, [ field, value, col, toDelete[col] ], deleteDone);
				});
				async.parallelLimit(deleteTasks, 1, (err) => {
					if (!err) {
						toDelete = {};
						toDeleteCount = 0;
					}
					done(err);
				});
			} else {
				done();
			}
		}

		return {
			add: function(obj, done) {
				let field = obj.__leo_delete__;
				let id = obj.__leo_delete_id__;
				if (id !== undefined && colLookup[field]) {
					if (!(field in toDelete)) {
						toDelete[field] = [];
					}
					toDelete[field].push(client.escapeValueNoToLower(id));
					toDeleteCount++;
					tryFlushDelete(done);
				} else {
					done();
				}
			},
			flush: (callback) => {
				tryFlushDelete(callback, true);
			}
		};
	}

	client.importFact = function(stream, table, ids, callback, tableDef = {}) {
		const stagingTable = `staging_${table}`;
		const qualifiedStagingTable = `\`${columnConfig.stageSchema}\`.\`${stagingTable}\``;
		const qualifiedTable = `\`${columnConfig.stageSchema}\`.\`${table}\``;
		if (!Array.isArray(ids)) {
			ids = [ids];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		schema[qualifiedStagingTable] = schema[qualifiedTable];

		let tasks = [];

		let deleteHandler = deletesSetup(qualifiedTable, schema[qualifiedTable], columnConfig._deleted, true);

		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		tasks.push(done => client.query(`drop table if exists ${columnConfig.stageSchema}.??`, [ `${stagingTable}_changes` ], done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ?? on ${qualifiedStagingTable} (??)`, [ `${stagingTable}_id`, ids ], done));
		//tasks.push(done => ls.pipe(stream, client.streamToTable(qualifiedStagingTable), done));
		tasks.push(done => {
			ls.pipe(stream, ls.through((obj, done, push) => {
				if (obj.__leo_delete__) {
					if (obj.__leo_delete__ == "id") {
						push(obj);
					}
					deleteHandler.add(obj, done);
				} else {
					done(null, obj);
				}
			}), client.streamToTable(stagingTable), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		// tasks.push(done => client.query(`analyze table ${qualifiedStagingTable}`, done));

		client.describeTable(table, (err, result) => {
			let columns = result.filter(field => !field.column_name.match(/^_/)).map(field => `${field.column_name}`);
			client.connect().then(connection => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let tasks = [];
					let totalRecords = 0;
					//The following code relies on the fact that now() will return the same time during all transaction events
					tasks.push(done => connection.query(`Start Transaction`, done));

					tasks.push(done => {
						connection.query(`select 1 as total from ${qualifiedTable} limit 1`, (err, results) => {
							if (err) {
								return done(err);
							}
							totalRecords = results.length;
							done();
						});
					});
					tasks.push(done => {
						const params = []
						ids.forEach(id => {
							params.push(id)
							params.push(id)
						})
						columns.forEach(column => {
							params.push(column)
							params.push(column)
							params.push(column)
						})
						connection.query(`Update ${qualifiedTable} prev
							join ${qualifiedStagingTable} staging on ${ids.map(() => `prev.?? = staging.??`).join(' and ')}
							SET  ${columns.map(() => `prev.?? = coalesce(staging.??, prev.??)`)}, prev.${columnConfig._deleted} = coalesce(prev.${columnConfig._deleted}, false), prev.${columnConfig._auditdate} = ${dwClient.auditdate}
							`, params, done);
					});


					//Now insert any we were missing
					tasks.push(done => {
						const params = [ [ ...columns, columnConfig._deleted, columnConfig._auditdate ] ]
						columns.forEach(column => {
							params.push(column)
							params.push(column)
						})
						ids.forEach(id => {
							params.push(id)
							params.push(id)
						})
						params.push(ids[0])
						connection.query(`INSERT INTO ${qualifiedTable} (??)
							SELECT ${columns.map(() => `coalesce(staging.??, prev.??)`)}, coalesce(prev.${columnConfig._deleted}, false), ${dwClient.auditdate} as ${columnConfig._auditdate}
							FROM ${qualifiedStagingTable} staging
							LEFT JOIN ${qualifiedTable} as prev on ${ids.map(() => `prev.?? = staging.??`).join(' and ')}
							WHERE prev.?? is null
							`, params, done);
					});
					tasks.push(done => connection.query(`drop table ${qualifiedStagingTable}`, done));

					async.series(tasks, err => {
						if (!err) {
							connection.query(`commit`, e => {
								connection.release();
								callback(e || err, {
									count: totalRecords
								});
							});
						} else {
							console.log('ROLLBACK import fact tasks error:', err)
							connection.query(`rollback`, (e, d) => {
								connection.release();
								callback(e, d);
							});
						}
					});
				});
			}).catch(callback);
		});
	};

	client.importDimension = function(stream, table, sk, nk, scds, callback, tableDef = {}) {
		const stagingTable = `staging_${table}`;
		const qualifiedStagingTable = `\`${columnConfig.stageSchema}\`.\`${stagingTable}\``;
		const qualifiedTable = `\`${columnConfig.stageSchema}\`.\`${table}\``;
		if (!Array.isArray(nk)) {
			nk = [nk];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		if (typeof schema[qualifiedTable] === 'undefined') {
			throw new Error(`${qualifiedTable} not found in schema`);
		}
		schema[qualifiedStagingTable] = schema[qualifiedTable].filter(c => c.column_name != sk);

		let tasks = [];
		let deleteHandler = deletesSetup(qualifiedTable, schema[qualifiedTable], columnConfig._enddate, dwClient.auditdate, `${columnConfig._current} = true`);

		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		tasks.push(done => client.query(`drop table if exists ${columnConfig.stageSchema}.??`, [ `${stagingTable}_changes` ], done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ?? on ${qualifiedStagingTable} (??)`, [ `${stagingTable}_id`, nk ], done));
		tasks.push(done => client.query(`alter table ${qualifiedStagingTable} drop column ??`, [ sk ], done));
		//tasks.push(done => ls.pipe(stream, client.streamToTable(qualifiedStagingTable), done));
		tasks.push(done => {
			ls.pipe(stream, ls.through((obj, done, push) => {
				if (obj.__leo_delete__) {
					if (obj.__leo_delete__ == "id") {
						push(obj);
					}
					deleteHandler.add(obj, done);
				} else {
					done(null, obj);
				}
			}), client.streamToTable(stagingTable), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		tasks.push(done => client.query(`analyze table ${qualifiedStagingTable}`, done));

		client.describeTable(table, (err, result) => {
			client.connect().then(connection => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					// let scd0 = scds[0] || []; // Not Used
					let scd2 = scds[2] || [];
					let scd3 = scds[3] || [];
					let scd6 = Object.keys(scds[6] || {});

					let ignoreColumns = [columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current];
					let allColumns = result.filter(field => {
						return ignoreColumns.indexOf(field.column_name) === -1 && field.column_name !== sk;
					}).map(r => r.column_name);

					let scd1 = result.map(r => r.column_name).filter(field => {
						return ignoreColumns.indexOf(field) === -1 && scd2.indexOf(field) === -1 && scd3.indexOf(field) === -1 && field !== sk && nk.indexOf(field) === -1;
					});


					let scdSQL = [];
					const params = [ `${stagingTable}_changes` ]
					nk.forEach(id => {
						params.push(id)
					})
					params.push(nk[0])

					//if (!scd2.length && !scd3.length && !scd6.length) {
					//	scdSQL.push(`1 as runSCD1`);
					//} else 
					if (scd1.length) {
						scdSQL.push(`CASE WHEN md5(${scd1.map(f => {
							params.push(f)
							return "md5(coalesce(staging.??,''))"
						}).join(' || ')}) = md5(${scd1.map(f => {
							params.push(f)
							return "md5(coalesce(prev.??,''))" 
						}).join(' || ')}) THEN 0 WHEN prev.?? is null then 0 ELSE 1 END as runSCD1`);
						params.push(nk[0])
					} else {
						scdSQL.push(`0 as runSCD1`);
					}
					if (scd2.length) {
						params.push(nk[0])
						scdSQL.push(`CASE WHEN prev.?? is null then 1 WHEN md5(${scd2.map(f => {
							params.push(f)
							return "md5(coalesce(staging.??,''))" 
						}).join(' || ')}) = md5(${scd2.map(f => {
							params.push(f)
							return "md5(coalesce(prev.??,''))" 
						}).join(' || ')}) THEN 0 ELSE 1 END as runSCD2`);
					} else {
						params.push(nk[0])
						scdSQL.push(`CASE WHEN prev.?? is null then 1 ELSE 0 END as runSCD2`);
					}
					if (scd3.length) {
						scdSQL.push(`CASE WHEN md5(${scd3.map(f => {
							params.push(f)
							return "md5(coalesce(staging.??,''))"
						}).join(' || ')}) = md5(${scd3.map(f => {
							params.push(f)
							return "md5(coalesce(prev.??,''))"
						}).join(' || ')}) THEN 0 WHEN prev.?? is null then 0 ELSE 1 END as runSCD3`);
						params.push(nk[0])
					} else {
						scdSQL.push(`0 as runSCD3`);
					}
					if (scd6.length) {
						scdSQL.push(`CASE WHEN md5(${scd6.map(f => {
							params.push(f)
							return "md5(coalesce(staging.??,''))"
						}).join(' || ')}) = md5(${scd6.map(f => {
							params.push(f)
							return "md5(coalesce(prev.??,''))"
						}).join(' || ')}) THEN 0 WHEN prev.?? is null then 0 ELSE 1 END as runSCD6`);
						params.push(nk[0])
					} else {
						scdSQL.push(`0 as runSCD6`);
					}
					
					nk.forEach(id => {
						params.push(id)
						params.push(id)
					})

					//let's figure out which SCDs needs to happen
					connection.query(`create table ${columnConfig.stageSchema}.?? as 
						select ${nk.map(() => `staging.??`)}, prev.?? is null as isNew,
						${scdSQL.join(',\n')}
						FROM ${qualifiedStagingTable} staging
						LEFT JOIN ${qualifiedTable} prev on ${nk.map(() => `prev.?? = staging.??`).join(' and ')} and prev.${columnConfig._current}`, params, function (err) {
						if (err) {
							console.log(err);
							process.exit();
						}
						let tasks = [];
						let totalRecords = 0;
						tasks.push(done => connection.query(`analyze table ${columnConfig.stageSchema}.??`, [ `${stagingTable}_changes` ], done));


						//The following code relies on the fact that now() will return the same time during all transaction events
						tasks.push(done => connection.query(`Start Transaction`, done));

						tasks.push(done => {
							const params = [ allColumns.concat([columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current]) ];
							allColumns.forEach(column => {
								params.push(column)
								params.push(column)
							})
							params.push(`${stagingTable}_changes`)
							nk.forEach(id => {
								params.push(id)
								params.push(id)
							})
							nk.forEach(id => {
								params.push(id)
								params.push(id)
							})
							connection.query(`INSERT INTO ${qualifiedTable} (??)
								SELECT ${allColumns.map(() => `coalesce(staging.??, prev.??)`).join(', ')}, ${dwClient.auditdate} as ${columnConfig._auditdate}, case when changes.isNew then '1900-01-01 00:00:00' else now() END as ${columnConfig._startdate}, '9999-01-01 00:00:00' as ${columnConfig._enddate}, true as ${columnConfig._current}
								FROM ${columnConfig.stageSchema}.?? changes
								JOIN ${qualifiedStagingTable} staging on ${nk.map(() => `staging.?? = changes.??`).join(' and ')}
								LEFT JOIN ${qualifiedTable} as prev on ${nk.map(() => `prev.?? = changes.??`).join(' and ')} and prev.${columnConfig._current}
								WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
								`, params, done);
						});

						//This needs to be done last
						tasks.push(done => {
							//RUN SCD1 / SCD6 columns  (where we update the old records)
							let columns = scd1.map(column => `prev.\`${column}\` = coalesce(staging.\`${column}\`, prev.\`${column}\`)`).concat(scd6.map(column => `prev.\`current_${column}\` = coalesce(staging.\`${column}\`, prev.\`${column}\`)`));
							columns.push(`prev.\`${columnConfig._enddate}\` = case when changes.runSCD2 =1 then now() else prev.\`${columnConfig._enddate}\` END`);
							columns.push(`prev.\`${columnConfig._current}\` = case when changes.runSCD2 =1 then false else prev.\`${columnConfig._current}\` END`);
							columns.push(`prev.\`${columnConfig._auditdate}\` = ${dwClient.auditdate}`);
							const params = [`${stagingTable}_changes`]
							connection.query(`update ${qualifiedTable} as prev
										JOIN ${columnConfig.stageSchema}.?? changes on ${nk.map(id=>`prev.${id} = changes.${id}`).join(' and ')}
										JOIN ${qualifiedStagingTable} staging on ${nk.map(id=>`staging.${id} = changes.${id}`).join(' and ')}
										set ${columns.join(', ')}
										where prev.${columnConfig._startdate} != now() and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having .${columnConfig._current}*/
											and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
										`, params, done);
						});

						tasks.push(done => connection.query(`drop table ${columnConfig.stageSchema}.??`, [ `${stagingTable}_changes` ], done));
						tasks.push(done => connection.query(`drop table ${qualifiedStagingTable}`, done));
						async.series(tasks, err => {
							if (!err) {
								connection.query(`commit`, e => {
									connection.release();
									callback(e || err, {
										count: totalRecords
									});
								});
							} else {
								console.log('ROLLBACK import dimension tasks error:', err)
								connection.query(`rollback`, (e, d) => {
									connection.release();
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
					unions[field.dimension].push(`select ${table}.${column} as id
						from ${columnConfig.stageSchema}.${table} left join ${columnConfig.stageSchema}.${field.dimension} on ${field.dimension}.${dimTableNk} = ${table}.${column}
						where ${field.dimension}.${dimTableNk} is null and ${table}.${columnConfig._auditdate} = ${dwClient.auditdate}`);
				}
			});
		});
		let missingDimTasks = Object.keys(unions).map(table => {
			let nk = tableNks[table][0];
			return (callback) => {
				let _auditdate = dwClient.auditdate; //results[0]._auditdate ? `'${results[0]._auditdate.replace(/^\d* */,"")}'` : "now()";
				let unionQuery = unions[table].join("\nUNION\n");
				client.query(`insert into ${columnConfig.stageSchema}.?? (??, ${columnConfig._auditdate}, ${columnConfig._startdate}, ${columnConfig._enddate}, ${columnConfig._current})
					select sub.id, max(${_auditdate}), '1900-01-01 00:00:00', '9999-01-01 00:00:00', true from (${unionQuery}) as sub where sub.id is not null group by sub.id`,
					[ table, nk ],
					(err) => {
						callback(err);
					}
				);
			};
		});
		async.parallelLimit(missingDimTasks, 10, (missingDimError) => {
			console.log(`Missing Dimensions ${!missingDimError && "Inserted"} ----------------------------`, missingDimError || "");
			callback(missingDimError);
		});

	};

	client.linkDimensions = function(table, links, nk, callback, tableStatus) {
		client.describeTable(table, (err) => {
			if (err) return callback(err);

			let tasks = [];

			// Only run analyze on the table if this is the first load
			if (tableStatus === "First Load") {
				tasks.push(done => client.query(`analyze table ${columnConfig.stageSchema}.??`, [ table ], done));
			}
			
			let sets = [];
			let setParams = [];
			let joinParams = [];

			tasks.push(done => {
				let joinTables = links.map(link => {
					if (link.table == "d_datetime" || link.table == "datetime" || link.table == "dim_datetime") {
						if (columnConfig.useSurrogateDateKeys) {
							setParams.push(`${link.destination}_date`)
							setParams.push(link.source)
							setParams.push(`${link.destination}_time`)
							setParams.push(link.source)
							sets.push(`t.?? = coalesce(DATE(t.??) - DATE('1400-01-01') + 10000, 1)`);
							sets.push(`t.?? = coalesce(TIME_TO_SEC(t.??) + 10000, 1)`);
						}
					} else if (link.table == "d_date" || link.table == "date" || link.table == "dim_date") {
						if (columnConfig.useSurrogateDateKeys) {
							setParams.push(`${link.destination}_date`)
							setParams.push(link.source)
							sets.push(`t.?? = coalesce(DATE(t.??) - DATE('1400-01-01') + 10000, 1)`);
						}
					} else if (link.table == "d_time" || link.table == "time" || link.table == "dim_time") {
						if (columnConfig.useSurrogateDateKeys) {
							setParams.push(`${link.destination}_time`)
							setParams.push(link.source)
							sets.push(`t.?? = coalesce(TIME_TO_SEC(t.??) + 10000, 1)`);
						}
					} else {
						setParams.push(link.destination)
						setParams.push(`${link.source}_join_table`)
						setParams.push(link.sk)
						sets.push(`t.?? = coalesce(??.??, 1)`);
						joinParams.push(...[
							link.table,
							`${link.source}_join_table`,
							`${link.source}_join_table`,
							link.on,
							link.source,
							link.link_date,
							`${link.source}_join_table`,
							link.link_date,
							`${link.source}_join_table`,
							`${link.source}_join_table`
						])
						return `LEFT JOIN ?? ??
							on ??.?? = t.??
								and t.?? >= ??.${columnConfig._startdate}
								and (t.?? <= ??.${columnConfig._enddate} or ??.${columnConfig._current})
					`;
					}
				});

				if (sets.length) {
					// join ${columnConfig.stageSchema}.?? t on ${nk.map(id => `dm.${id} = t.${id}`).join(' and ')}
					client.query(`Update ${columnConfig.stageSchema}.?? t
                        ${joinTables.join("\n")}
                        SET ${sets.join(', ')}
                        where t.${columnConfig._auditdate} = ${dwClient.auditdate}
                    `, [ table, ...joinParams, ...setParams ], done);
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
						if (!structures[table].isDimension) {
							structures[table].structure[columnConfig._deleted] = structures[table].structure[columnConfig._deleted] || "boolean";
						}
						Object.keys(structures[table].structure).forEach(field => {
							if (!(field in fieldLookup)) {
								missingFields[field] = structures[table].structure[field];
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
		let dbType = config.type.toLowerCase();
		let defQueries = definition.queries;
		if (defQueries && !Array.isArray(defQueries)) {
			defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version];
		}
		let queries = [].concat(defQueries || []);

		let ids = [];
		const params = [];
        Object.keys(definition.structure).forEach(key => {
			let field = definition.structure[key];
			if (field == "sk") {
				field = {
					type: 'integer NOT NULL AUTO_INCREMENT primary key'
				};
			} else if (typeof field == "string") {
				field = {
					type: field
				};
			}

			if (field == "nk" || field.nk) {
				ids.push(key);
			}
			if (field.queries) {
				let defQueries = field.queries;
				if (!Array.isArray(defQueries)) {
					defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version] || [];
				}
				queries = queries.concat(defQueries);
			}

			if (field.dimension == "d_datetime" || field.dimension == "datetime" || field.dimension == "dim_datetime") {
				if (columnConfig.useSurrogateDateKeys) {
					params.push(`${columnConfig.dimColumnTransform(key, field)}_date`)
					fields.push(`?? integer`);
					params.push(`${columnConfig.dimColumnTransform(key, field)}_time`)
					fields.push(`?? integer`);
				}
			} else if (field.dimension == "d_date" || field.dimension == "date" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					params.push(`${columnConfig.dimColumnTransform(key, field)}_date`)
					fields.push(`?? integer`);
				}
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_time") {
				if (columnConfig.useSurrogateDateKeys) {
					params.push(`${columnConfig.dimColumnTransform(key, field)}_time`)
					fields.push(`?? integer`);
				}
			} else if (field.dimension) {
				params.push(`${columnConfig.dimColumnTransform(key, field)}`)
				fields.push(`?? integer`);
			}
			params.push(key)
			fields.push(`?? ${field.type}`);
		});

		let sql = `create table ${columnConfig.stageSchema}.?? (
				${fields.join(',\n')}
			)`;


		/*
			@todo if dimension, add empty row
		*/
		let tasks = [];
        tasks.push(done => client.query(sql, [ table, ...params], done));

		if (definition.isDimension) {
            tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._auditdate} timestamp`, [ table ], done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._startdate} timestamp`, [ table ], done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._enddate} timestamp`, [ table ], done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._current} boolean`, [ table ], done));
			tasks.push(done => client.query(`create index ?? on ${columnConfig.stageSchema}.?? (??)`, [ `${table}_bk`, table, ids.concat(columnConfig._current) ], done));
			tasks.push(done => client.query(`create index ?? on ${columnConfig.stageSchema}.?? (??)`, [ `${table}_bk2`, table, ids.concat(columnConfig._startdate) ], done));
			tasks.push(done => client.query(`create index ?? on ${columnConfig.stageSchema}.?? (${columnConfig._auditdate})`, [ `${table}${columnConfig._auditdate}`, table ], done));
		} else {
            tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._auditdate} timestamp`, [ table ], done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.?? add column ${columnConfig._deleted} boolean`, [ table ], done));
			tasks.push(done => client.query(`create index ?? on ${columnConfig.stageSchema}.?? (${columnConfig._auditdate})`, [ `${table}${columnConfig._auditdate}`, table ], done));
			tasks.push(done => client.query(`create index ?? on ${columnConfig.stageSchema}.?? (??)`, [ `${table}_bk`, table, ids ], done));
		}
		queries.map(q => {
			tasks.push(done => client.query(q, err => done(err)));
		});
		async.series(tasks, callback);
	};
	client.updateTable = function(table, definition, callback) {
		let fields = [];
		let queries = [];
		Object.keys(definition).forEach(key => {
			let field = definition[key];
			if (field == "sk") {
				field = {
					type: 'integer primary key'
				};
			} else if (typeof field == "string") {
				field = {
					type: field
				};
			}
			if (field.queries) {
				let defQueries = field.queries;
				if (!Array.isArray(defQueries)) {
					let dbType = config.type.toLowerCase();
					defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version] || [];
				}
				queries = queries.concat(defQueries);
			}

			if (field.dimension == "d_datetime" || field.dimension == "datetime" || field.dimension == "dim_datetime") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push({ key: `${columnConfig.dimColumnTransform(key, field)}_date`, type: `integer` });
					fields.push({ key: `${columnConfig.dimColumnTransform(key, field)}_time`, type: `integer` });
				}
			} else if (field.dimension == "d_date" || field.dimension == "date" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push({ key: `${columnConfig.dimColumnTransform(key, field)}_date`, type: `integer` });
				}
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push({ key: `${columnConfig.dimColumnTransform(key, field)}_time`, type: `integer` });
				}
			} else if (field.dimension) {
				fields.push({ key: `${columnConfig.dimColumnTransform(key, field)}`, type: `integer` });
			}
			fields.push({ key, type: field.type });
		});

		client.query(`alter table ?? ${fields.map(field => `add column ?? ${field.type}`).join(`,\n`)}`, [ table, ...fields.map(i => i.key) ], callback);
	};

	client.findAuditDate = function(table, callback) {
		client.query(`select to_char(max(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as max FROM ${columnConfig.stageSchema}.??`, [ table ], (err, auditdate) => {
			if (err) {
				callback(err);
			} else {
				let audit = auditdate && auditdate[0].max;
				let auditdateCompare = audit != null ? `${columnConfig._auditdate} >= ${client.escapeValue(audit)}` : `${columnConfig._auditdate} is null`;
				client.query(`select count(*) as count FROM ${columnConfig.stageSchema}.?? where ${auditdateCompare}`, [ table ], (err, count) => {
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
		client.query(`select count(*) as count FROM ${columnConfig.stageSchema}.?? WHERE ${auditdateCompare}`, [ table ], (err, result) => {
			let where = "";

			let mysqlAuditDate = parseInt(result[0].count);

			if (remoteAuditdate.auditdate && mysqlAuditDate <= remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} > ${client.escapeValue(remoteAuditdate.auditdate)}`;
			} else if (remoteAuditdate.auditdate && mysqlAuditDate > remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} >= ${client.escapeValue(remoteAuditdate.auditdate)}`;
			}
			client.query(`select to_char(min(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as oldest, count(*) as count
				FROM ${columnConfig.stageSchema}.??
				${where}
				`, [ table ], (err, result) => {

				if (result[0].count) {
					let totalCount = parseInt(result[0].count);
					let needs = {
						[columnConfig._auditdate]: true,
						[columnConfig._current]: opts.isDimension,
						[columnConfig._startdate]: opts.isDimension,
						[columnConfig._enddate]: opts.isDimension

					};
					var field = fields.map(field => {
						needs[field] = false;
						return field;
					});
					Object.keys(needs).map(key => {
						if (needs[key]) {
							field.push(key);
						}
					});

					if (config.version == "redshift") {
						let fileBase = `s3://${opts.bucket}${opts.file}/${table}`;
						let query = `UNLOAD ('select ?? from ${columnConfig.stageSchema}.?? ${where}') to '${fileBase}' MANIFEST OVERWRITE ESCAPE iam_role '${opts.role}';`;
						client.query(query, [ field, table ], (err) => {
							callback(err, fileBase + ".manifest", totalCount, result[0].oldest);
						});
					} else {
						let file = `${opts.file}/${table}.csv`;
						ls.pipe(client.streamFromTable(table, {
							columns: field,
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

	return client;
};

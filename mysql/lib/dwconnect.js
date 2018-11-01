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
			return field.dim_column ? field.dim_column : `d_${column.replace(/_id$/,'').replace(/^d_/,'')}`;
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
					return deleteDone => client.query(`update ${qualifiedTable} set ${field} = ? where ? in (?) ${where}`, [ value, col, toDelete[col] ], deleteDone);
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
		const qualifiedStagingTable = `${columnConfig.stageSchema}.${stagingTable}`;
		const qualifiedStagingChangesTable = `${stagingTable}_changes`;
		const qualifiedTable = `${columnConfig.stageSchema}.${table}`;
		if (!Array.isArray(ids)) {
			ids = [ids];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		schema[qualifiedStagingTable] = schema[qualifiedTable];

		let tasks = [];

		let deleteHandler = deletesSetup(qualifiedTable, schema[qualifiedTable], columnConfig._deleted, true);

		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingChangesTable}`, done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ${stagingTable}_id on ${qualifiedStagingTable} (??)`, [ ids ], done));
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
						connection.query(`Update ${qualifiedTable} prev
							join ${qualifiedStagingTable} staging on ${ids.map(id => `prev.${client.escapeId(id)} = staging.${client.escapeId(id)}`).join(' and ')}
							SET  ${columns.map(column => `prev.${client.escapeId(column)} = coalesce(staging.${client.escapeId(column)}, prev.${client.escapeId(column)})`)}, prev.${columnConfig._deleted} = coalesce(prev.${columnConfig._deleted}, false), prev.${columnConfig._auditdate} = ${dwClient.auditdate}
							`, done);
					});

					//Now insert any we were missing
					tasks.push(done => {
						const params = [ [ ...columns, columnConfig._deleted, columnConfig._auditdate ] ];
						connection.query(`INSERT INTO ${qualifiedTable} (??)
							SELECT ${columns.map(column => `coalesce(staging.${client.escapeId(column)}, prev.${client.escapeId(column)})`)}, coalesce(prev.${columnConfig._deleted}, false), ${dwClient.auditdate} as ${columnConfig._auditdate}
							FROM ${qualifiedStagingTable} staging
							LEFT JOIN ${qualifiedTable} as prev on ${ids.map(id => `prev.${client.escapeId(id)} = staging.${client.escapeId(id)}`).join(' and ')}
							WHERE prev.${ids[0]} is null
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
							console.log('ROLLBACK import fact tasks error:', err);
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
		const qualifiedStagingTable = `${columnConfig.stageSchema}.${stagingTable}`;
		const qualifiedStagingChangesTable = `${stagingTable}_changes`;
		const qualifiedTable = `${columnConfig.stageSchema}.${table}`;
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
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingChangesTable}`, done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ${stagingTable}_id on ${qualifiedStagingTable} (??)`, [ nk ], done));
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
						return !ignoreColumns.includes(field.column_name) && field.column_name !== sk;
					}).map(r => r.column_name);

					let scd1 = result.map(r => r.column_name).filter(field => {
						return !ignoreColumns.includes(field) && !scd2.includes(field) && !scd3.includes(field) && field !== sk && !nk.includes(field);
					});


					let scdSQL = [];
					const params = [];

					//if (!scd2.length && !scd3.length && !scd6.length) {
					//	scdSQL.push(`1 as runSCD1`);
					//} else 
					if (scd1.length) {
						scdSQL.push(`CASE WHEN md5(concat(${scd1.map(f => `md5(coalesce(staging.${f},''))`).join(', ')})) = md5(concat(${scd1.map(f => `md5(coalesce(prev.${f},''))`).join(', ')})) THEN 0 WHEN prev.${nk[0]} is null then 0 ELSE 1 END as runSCD1`);
					} else {
						scdSQL.push(`0 as runSCD1`);
					}
					if (scd2.length) {
						scdSQL.push(`CASE WHEN prev.${nk[0]} is null then 1 WHEN md5(concat(${scd2.map(f => `md5(coalesce(staging.${f},''))`).join(', ')})) = md5(concat(${scd2.map(f => `md5(coalesce(prev.${f},''))`).join(', ')})) THEN 0 ELSE 1 END as runSCD2`);
					} else {
						scdSQL.push(`CASE WHEN prev.${nk[0]} is null then 1 ELSE 0 END as runSCD2`);
					}
					if (scd3.length) {
						scdSQL.push(`CASE WHEN md5(concat(${scd3.map(f => `md5(coalesce(staging.${f},''))`).join(', ')})) = md5(concat(${scd3.map(f => `md5(coalesce(prev.${f},''))`).join(', ')})) THEN 0 WHEN prev.${nk[0]} is null then 0 ELSE 1 END as runSCD3`);
					} else {
						scdSQL.push(`0 as runSCD3`);
					}
					if (scd6.length) {
						scdSQL.push(`CASE WHEN md5(concat(${scd6.map(f => `md5(coalesce(staging.${f},''))`).join(', ')})) = md5(concat(${scd6.map(f => `md5(coalesce(prev.${f},''))`).join(', ')})) THEN 0 WHEN prev.${nk[0]} is null then 0 ELSE 1 END as runSCD6`);
					} else {
						scdSQL.push(`0 as runSCD6`);
					}
					
					//let's figure out which SCDs needs to happen
					connection.query(`create table ${qualifiedStagingChangesTable} as 
						select ${nk.map(id => `staging.${client.escapeId(id)}`)}, prev.${nk[0]} is null as isNew,
						${scdSQL.join(',\n')}
						FROM ${qualifiedStagingTable} staging
						LEFT JOIN ${qualifiedTable} prev on ${nk.map(id => `prev.${client.escapeId(id)} = staging.${client.escapeId(id)}`).join(' and ')} and prev.${columnConfig._current}`, params, function (err) {
						if (err) {
							process.exit();
						}
						let tasks = [];
						let totalRecords = 0;
						tasks.push(done => connection.query(`analyze table ${qualifiedStagingChangesTable}`, done));


						//The following code relies on the fact that now() will return the same time during all transaction events
						tasks.push(done => connection.query(`Start Transaction`, done));

						tasks.push(done => {
							const params = [ allColumns.concat([columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current]) ];
							connection.query(`INSERT INTO ${qualifiedTable} (??)
							SELECT ${allColumns.map(column => `coalesce(staging.${client.escapeId(column)}, prev.${client.escapeId(column)})`).join(', ')}, ${dwClient.auditdate} as ${columnConfig._auditdate}, case when changes.isNew then '1900-01-01 00:00:00' else now() END as ${columnConfig._startdate}, '9999-01-01 00:00:00' as ${columnConfig._enddate}, true as ${columnConfig._current}
							FROM ${qualifiedStagingChangesTable} changes
							JOIN ${qualifiedStagingTable} staging on ${nk.map(id => `staging.${client.escapeId(id)} = changes.${client.escapeId(id)}`).join(' and ')}
							LEFT JOIN ${qualifiedTable} as prev on ${nk.map(id => `prev.${client.escapeId(id)} = changes.${client.escapeId(id)}`).join(' and ')} and prev.${columnConfig._current}
							WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
							`, params, done);
						});

						//This needs to be done last
						tasks.push(done => {
							//RUN SCD1 / SCD6 columns  (where we update the old records)
							let columns = scd1.map(column => `prev.${client.escapeId(column)} = coalesce(staging.${client.escapeId(column)}, prev.${client.escapeId(column)})`).concat(scd6.map(column => `prev.\`current_${column}\` = coalesce(staging.${client.escapeId(column)}, prev.${client.escapeId(column)})`));
							columns.push(`prev.\`${columnConfig._enddate}\` = case when changes.runSCD2 =1 then now() else prev.\`${columnConfig._enddate}\` END`);
							columns.push(`prev.\`${columnConfig._current}\` = case when changes.runSCD2 =1 then false else prev.\`${columnConfig._current}\` END`);
							columns.push(`prev.\`${columnConfig._auditdate}\` = ${dwClient.auditdate}`);
							connection.query(`update ${qualifiedTable} as prev
							JOIN ${qualifiedStagingChangesTable} changes on ${nk.map(id=>`prev.${client.escapeId(id)} = changes.${client.escapeId(id)}`).join(' and ')}
							JOIN ${qualifiedStagingTable} staging on ${nk.map(id=>`staging.${client.escapeId(id)} = changes.${client.escapeId(id)}`).join(' and ')}
							set ${columns.join(', ')}
							where prev.${columnConfig._startdate} != now() and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having .${columnConfig._current}*/
								and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
							`, done);
						});

						tasks.push(done => connection.query(`drop table ${qualifiedStagingChangesTable}`, done));
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
								console.log('ROLLBACK import dimension tasks error:', err);
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
					unions[field.dimension].push(`select ${table}.${client.escapeId(column)} as id
						from ${columnConfig.stageSchema}.${table} left join ${columnConfig.stageSchema}.${field.dimension} on ${field.dimension}.${dimTableNk} = ${table}.${client.escapeId(column)}
						where ${field.dimension}.${dimTableNk} is null and ${table}.${columnConfig._auditdate} = ${dwClient.auditdate}`);
				}
			});
		});
		let missingDimTasks = Object.keys(unions).map(table => {
			let nk = tableNks[table][0];
			return (callback) => {
				let _auditdate = dwClient.auditdate; //results[0]._auditdate ? `'${results[0]._auditdate.replace(/^\d* */,"")}'` : "now()";
				let unionQuery = unions[table].join("\nUNION\n");
				client.query(`insert into ${columnConfig.stageSchema}.${table} (${nk}, ${columnConfig._auditdate}, ${columnConfig._startdate}, ${columnConfig._enddate}, ${columnConfig._current})
					select sub.id, max(${_auditdate}), '1900-01-01 00:00:00', '9999-01-01 00:00:00', true from (${unionQuery}) as sub where sub.id is not null group by sub.id`,
				(err) => {
					callback(err);
				});
			};
		});
		async.parallelLimit(missingDimTasks, 10, (missingDimError) => {
			console.log(`Missing Dimensions ${!missingDimError && "Inserted"} ----------------------------`, missingDimError || "");
			callback(missingDimError);
		});

	};

	client.linkDimensions = function(table, links, nk, callback, tableStatus) {
		const qualifiedTable = `${columnConfig.stageSchema}.${table}`;
		client.describeTable(table, (err) => {
			if (err) return callback(err);

			let tasks = [];

			// Only run analyze on the table if this is the first load
			if (tableStatus === "First Load") {
				tasks.push(done => client.query(`analyze table ${qualifiedTable}`, done));
			}
			
			let sets = [];

			tasks.push(done => {
				let joinTables = links.map(link => {
					if (link.table == "d_datetime" || link.table == "datetime" || link.table == "dim_datetime") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`t.${link.destination}_date = coalesce(DATEDIFF(t.${link.source}, '1400-01-01') + 10000, 1)`);
							sets.push(`t.${link.destination}_time = coalesce(TIME_TO_SEC(t.${link.source}) + 10000, 1)`);
						}
					} else if (link.table == "d_date" || link.table == "date" || link.table == "dim_date") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`t.${link.destination}_date = coalesce(DATEDIFF(t.${link.source}, '1400-01-01') + 10000, 1)`);
						}
					} else if (link.table == "d_time" || link.table == "time" || link.table == "dim_time") {
						if (columnConfig.useSurrogateDateKeys) {
							sets.push(`t.${link.destination}_time = coalesce(TIME_TO_SEC(t.${link.source}) + 10000, 1)`);
						}
					} else {
						sets.push(`t.${link.destination} = coalesce(${link.source}_join_table.${link.sk}, 1)`);
						return `LEFT JOIN ${link.table} ${link.source}_join_table
							on ${link.source}_join_table.${link.on} = t.${link.source}
								and t.${link.link_date} >= ${link.source}_join_table.${columnConfig._startdate}
								and (t.${link.link_date} <= ${link.source}_join_table.${columnConfig._enddate} or ${link.source}_join_table.${columnConfig._current})
					`;
					}
				});

				if (sets.length) {
					// join ${columnConfig.stageSchema}.?? t on ${nk.map(id => `dm.${client.escapeId(id)} = t.${client.escapeId(id)}`).join(' and ')}
					client.query(`Update ${qualifiedTable} t
                        ${joinTables.join("\n")}
                        SET ${sets.join(', ')}
                        where t.${columnConfig._auditdate} = ${dwClient.auditdate}
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
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
				}
			} else if (field.dimension == "d_date" || field.dimension == "date" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
				}
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_time") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
				}
			} else if (field.dimension) {
				fields.push(`${columnConfig.dimColumnTransform(key, field)} integer`);
			}
			fields.push(`${client.escapeId(key)} ${field.type}`);
		});

		let sql = `create table ${columnConfig.stageSchema}.${table} (
				${fields.join(',\n')}
			)`;


		/*
			@todo if dimension, add empty row
		*/
		let tasks = [];
		tasks.push(done => client.query(sql, done));

		if (definition.isDimension) {
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._auditdate} timestamp null`, done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._startdate} timestamp null`, done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._enddate} timestamp null`, done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._current} boolean`, done));
			tasks.push(done => client.query(`create index ${table}_bk on ${columnConfig.stageSchema}.${table} (??)`, [ ids.concat(columnConfig._current) ], done));
			tasks.push(done => client.query(`create index ${table}_bk2 on ${columnConfig.stageSchema}.${table} (??)`, [ ids.concat(columnConfig._startdate) ], done));
			tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${columnConfig.stageSchema}.${table} (${columnConfig._auditdate})`, done));
		} else {
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._auditdate} timestamp null`, done));
			tasks.push(done => client.query(`alter table ${columnConfig.stageSchema}.${table} add column ${columnConfig._deleted} boolean`, done));
			tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${columnConfig.stageSchema}.${table} (${columnConfig._auditdate})`, done));
			tasks.push(done => client.query(`create index ${table}_bk on ${columnConfig.stageSchema}.${table} (??)`, [ ids ], done));
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

		client.query(`alter table ${columnConfig.stageSchema}.${table} ${fields.map(field => `add column ${field.key} ${field.type}`).join(`,\n`)}`, callback);
	};

	client.findAuditDate = function(table, callback) {
		client.query(`select to_char(max(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as max FROM ${columnConfig.stageSchema}.${table}`, (err, auditdate) => {
			if (err) {
				callback(err);
			} else {
				let audit = auditdate && auditdate[0].max;
				let auditdateCompare = audit != null ? `${columnConfig._auditdate} >= ${client.escapeValue(audit)}` : `${columnConfig._auditdate} is null`;
				client.query(`select count(*) as count FROM ${columnConfig.stageSchema}.${table} where ${auditdateCompare}`, (err, count) => {
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
		client.query(`select count(*) as count FROM ${columnConfig.stageSchema}.${table} WHERE ${auditdateCompare}`, (err, result) => {
			let where = "";

			let mysqlAuditDate = parseInt(result[0].count);

			if (remoteAuditdate.auditdate && mysqlAuditDate <= remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} > ${client.escapeValue(remoteAuditdate.auditdate)}`;
			} else if (remoteAuditdate.auditdate && mysqlAuditDate > remoteAuditdate.count) {
				where = `WHERE ${columnConfig._auditdate} >= ${client.escapeValue(remoteAuditdate.auditdate)}`;
			}
			client.query(`select to_char(min(${columnConfig._auditdate}), 'YYYY-MM-DD HH24:MI:SS') as oldest, count(*) as count
				FROM ${columnConfig.stageSchema}.${table}
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
						let query = `UNLOAD ('select ?? from ${columnConfig.stageSchema}.${table} ${where}') to '${fileBase}' MANIFEST OVERWRITE ESCAPE iam_role '${opts.role}';`;
						client.query(query, [ field ], (err) => {
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

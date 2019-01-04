"use strict";
const postgres = require("./connect.js");
const async = require("async");
const ls = require("leo-sdk").streams;

module.exports = function (config, columnConfig) {
	let client = postgres(config);
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
			return field.dim_column ? field.dim_column : `d_${column.replace(/_id$/, '').replace(/^d_/, '')}`;
		},
		useSurrogateDateKeys: true,
		stageSchema: 'public'
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
					return deleteDone => client.query(`update ${qualifiedTable} set ${field} = ${value} where ${col} in (${toDelete[col].join(",")}) ${where}`, deleteDone);
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
			add: function (obj, done) {
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

	client.importFact = function (stream, table, ids, callback, tableDef = {}) {
		const stagingTable = `staging_${table}`;
		const qualifiedStagingTable = `${columnConfig.stageSchema}.${stagingTable}`;
		const qualifiedTable = `public.${table}`;
		if (!Array.isArray(ids)) {
			ids = [ids];
		}

		// Add the new table to in memory schema // Prevents locking on schema table
		let schema = client.getSchemaCache();
		schema[qualifiedStagingTable] = schema[qualifiedTable];

		let tasks = [];

		let deleteHandler = deletesSetup(qualifiedTable, schema[qualifiedTable], columnConfig._deleted, true);

		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}_changes`, done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ${stagingTable}_id on ${qualifiedStagingTable} (${ids.join(', ')})`, done));
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
			}), client.streamToTable(qualifiedStagingTable), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		tasks.push(done => client.query(`analyze ${qualifiedStagingTable}`, done));

		client.describeTable(table, (err, result) => {
			let columns = result.filter(field => !field.column_name.match(/^_/)).map(field => `"${field.column_name}"`);

			client.connect().then(connection => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let tasks = [];
					let totalRecords = 0;
					//The following code relies on the fact that now() will return the same time during all transaction events
					tasks.push(done => connection.query(`Begin Transaction`, done));
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
								SET  ${columns.map(column => `${column} = coalesce(staging.${column}, prev.${column})`)}, ${columnConfig._deleted} = coalesce(prev.${columnConfig._deleted}, false), ${columnConfig._auditdate} = ${dwClient.auditdate}
								FROM ${qualifiedStagingTable} staging
								where ${ids.map(id => `prev.${id} = staging.${id}`).join(' and ')}
							`, done);
					});


					//Now insert any we were missing
					tasks.push(done => {
						connection.query(`INSERT INTO ${qualifiedTable} (${columns.join(',')},${columnConfig._deleted},${columnConfig._auditdate})
								SELECT ${columns.map(column => `coalesce(staging.${column}, prev.${column})`)}, coalesce(prev.${columnConfig._deleted}, false), ${dwClient.auditdate} as ${columnConfig._auditdate}
								FROM ${qualifiedStagingTable} staging
								LEFT JOIN ${qualifiedTable} as prev on ${ids.map(id => `prev.${id} = staging.${id}`).join(' and ')}
								WHERE prev.${ids[0]} is null	
							`, done);
					});
					// tasks.push(done => connection.query(`drop table ${stagingTbl}`, done));

					async.series(tasks, err => {
						if (!err) {
							connection.query(`commit`, e => {
								connection.release();
								callback(e || err, {
									count: totalRecords
								});
							});
						} else {
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

	client.importDimension = function (stream, table, sk, nk, scds, callback, tableDef = {}) {
		const stagingTbl = `staging_${table}`;
		const qualifiedStagingTable = `${columnConfig.stageSchema}.${stagingTbl}`;
		const qualifiedTable = `public.${table}`;
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
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}_changes`, done));
		tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
		tasks.push(done => client.query(`create index ${stagingTbl}_id on ${qualifiedStagingTable} (${nk.join(', ')})`, done));
		tasks.push(done => client.query(`alter table ${qualifiedStagingTable} drop column ${sk}`, done));
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
			}), client.streamToTable(qualifiedStagingTable), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		tasks.push(done => client.query(`analyze ${qualifiedStagingTable}`, done));

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
					}).map(r => `"${r.column_name}"`);

					let scd1 = result.map(r => r.column_name).filter(field => {
						return ignoreColumns.indexOf(field) === -1 && scd2.indexOf(field) === -1 && scd3.indexOf(field) === -1 && field !== sk && nk.indexOf(field) === -1;
					});


					let scdSQL = [];

					//if (!scd2.length && !scd3.length && !scd6.length) {
					//	scdSQL.push(`1 as runSCD1`);
					//} else 
					if (scd1.length) {
						scdSQL.push(`CASE WHEN md5(${scd1.map(f => "md5(coalesce(s." + f + "::text,''))").join(' || ')}) = md5(${scd1.map(f => "md5(coalesce(d." + f + "::text,''))").join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD1`);
					} else {
						scdSQL.push(`0 as runSCD1`);
					}
					if (scd2.length) {
						scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 WHEN md5(${scd2.map(f => "md5(coalesce(s." + f + "::text,''))").join(' || ')}) = md5(${scd2.map(f => "md5(coalesce(d." + f + "::text,''))").join(' || ')}) THEN 0 ELSE 1 END as runSCD2`);
					} else {
						scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 ELSE 0 END as runSCD2`);
					}
					if (scd3.length) {
						scdSQL.push(`CASE WHEN md5(${scd3.map(f => "md5(coalesce(s." + f + "::text,''))").join(' || ')}) = md5(${scd3.map(f => "md5(coalesce(d." + f + "::text,''))").join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD3`);
					} else {
						scdSQL.push(`0 as runSCD3`);
					}
					if (scd6.length) {
						scdSQL.push(`CASE WHEN md5(${scd6.map(f => "md5(coalesce(s." + f + "::text,''))").join(' || ')}) = md5(${scd6.map(f => "md5(coalesce(d." + f + "::text,''))").join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD6`);
					} else {
						scdSQL.push(`0 as runSCD6`);
					}

					//let's figure out which SCDs needs to happen
					connection.query(`create table ${qualifiedStagingTable}_changes as 
				select ${nk.map(id => `s.${id}`).join(', ')}, d.${nk[0]} is null as isNew,
					${scdSQL.join(',\n')}
					FROM ${qualifiedStagingTable} s
					LEFT JOIN ${qualifiedTable} d on ${nk.map(id => `d.${id} = s.${id}`).join(' and ')} and d.${columnConfig._current}`, (err) => {
						if (err) {
							console.log(err);
							process.exit();
						}
						let tasks = [];
						let rowId = null;
						let totalRecords = 0;
						tasks.push(done => connection.query(`analyze ${qualifiedStagingTable}_changes`, done));
						tasks.push(done => {
							connection.query(`select max(${sk}) as maxid from ${qualifiedTable}`, (err, results) => {
								if (err) {
									return done(err);
								}
								rowId = results[0].maxid || 10000;
								totalRecords = (rowId - 10000);
								done();
							});
						});


						//The following code relies on the fact that now() will return the same time during all transaction events
						tasks.push(done => connection.query(`Begin Transaction`, done));

						tasks.push(done => {
							let fields = [sk].concat(allColumns).concat([columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current]);
							connection.query(`INSERT INTO ${qualifiedTable} (${fields.join(',')})
								SELECT row_number() over () + ${rowId}, ${allColumns.map(column => `coalesce(staging.${column}, prev.${column})`)}, ${dwClient.auditdate} as ${columnConfig._auditdate}, case when changes.isNew then '1900-01-01 00:00:00' else now() END as ${columnConfig._startdate}, '9999-01-01 00:00:00' as ${columnConfig._enddate}, true as ${columnConfig._current}
								FROM ${qualifiedStagingTable}_changes changes  
								JOIN ${qualifiedStagingTable} staging on ${nk.map(id => `staging.${id} = changes.${id}`).join(' and ')}
								LEFT JOIN ${qualifiedTable} as prev on ${nk.map(id => `prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._current}
								WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
								`, done);
						});

						//This needs to be done last
						tasks.push(done => {
							//RUN SCD1 / SCD6 columns  (where we update the old records)
							let columns = scd1.map(column => `"${column}" = coalesce(staging."${column}", prev."${column}")`).concat(scd6.map(column => `"current_${column}" = coalesce(staging."${column}", prev."${column}")`));
							columns.push(`"${columnConfig._enddate}" = case when changes.runSCD2 =1 then now() else prev."${columnConfig._enddate}" END`);
							columns.push(`"${columnConfig._current}" = case when changes.runSCD2 =1 then false else prev."${columnConfig._current}" END`);
							columns.push(`"${columnConfig._auditdate}" = ${dwClient.auditdate}`);
							connection.query(`update ${qualifiedTable} as prev
										set  ${columns.join(', ')}
										FROM ${qualifiedStagingTable}_changes changes
										JOIN ${qualifiedStagingTable} staging on ${nk.map(id => `staging.${id} = changes.${id}`).join(' and ')}
										where ${nk.map(id => `prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._startdate} != now() and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having .${columnConfig._current}*/
											and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
										`, done);
						});

						tasks.push(done => connection.query(`drop table ${qualifiedStagingTable}_changes`, done));
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

	client.insertMissingDimensions = function (usedTables, tableConfig, tableSks, tableNks, callback) {
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
					const factTable = table;
					if (!field.on) {
						const dimId = field.dim_column.replace(/_key$/, '_id');
						field.on = {
							[dimId]: dimId
						};
					}
					if (!(unions[field.dimension])) {
						unions[field.dimension] = [];
					}
					if (typeof tableNks[field.dimension] === 'undefined') {
						throw new Error(`${field.dimension} not found in tableNks`);
					}
					const factNks = Object.keys(field.on);
					unions[field.dimension].push(`
						select ${factNks.map((fnk, i)=> `${factTable}.${fnk} as id${i}`).join(', ')} 
						from ${factTable} 
							left join ${field.dimension} 
								on ${factNks.map((fnk)=> `${factTable}.${fnk} = ${field.dimension}.${field.on[fnk]}`).join(' AND ')} 
						where ${factNks.map((fnk)=> `${field.dimension}.${field.on[fnk]} is null`).join(' AND ')} 
							and ${factTable}.${columnConfig._auditdate} = ${dwClient.auditdate}
					`);
				}
			});
		});
		let missingDimTasks = Object.keys(unions).map(table => {
			let sk = tableSks[table];
			let nks = tableNks[table];
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
						const insertQuery = `
							insert into ${table} (${sk}, ${nks.join(', ')}, ${columnConfig._auditdate}, ${columnConfig._startdate}, ${columnConfig._enddate}, ${columnConfig._current}) 
							select row_number() over () + ${rowId}, ${nks.map((_, i)=> `sub.id${i}`).join(', ')}, max(${_auditdate})::timestamp, '1900-01-01 00:00:00', '9999-01-01 00:00:00', true from (${unionQuery}) as sub 
							where ${nks.map((_, i)=> `sub.id${i} is not null`).join(' OR ')} group by ${nks.map((_, i)=> `sub.id${i}`).join(', ')}`; 
						// console.log(insertQuery)
						transaction.query(insertQuery, (err) => {
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

	client.linkDimensions = function (table, links, nk, callback, tableStatus) {
		client.describeTable(table, (err) => {
			if (err) return callback(err);

			let tasks = [];
			let sets = [];

			const linkAuditdate = client.escapeValueNoToLower(new Date().toISOString().replace(/\.\d*Z/, "Z"));

			// Only run analyze on the table if this is the first load
			if (tableStatus === "First Load") {
				tasks.push(done => client.query(`analyze ${table}`, done));
			}
			tasks.push(done => {
				let joinTables = links.map(link => {
					if (columnConfig.useSurrogateDateKeys && (link.table == "d_datetime" || link.table == "datetime" || link.table == "dim_datetime")) {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else if (columnConfig.useSurrogateDateKeys && (link.table == "d_date" || link.table == "date" || link.table == "dim_date")) {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
					} else if (columnConfig.useSurrogateDateKeys && (link.table == "d_time" || link.table == "time" || link.table == "dim_time")) {
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else {
						sets.push(`${link.destination} = coalesce(${link.join_id}_join_table.${link.sk}, 1)`);
						var joinOn = `${link.join_id}_join_table.${link.on} = t.${link.source}`;
						if (Array.isArray(link.source)) {
							joinOn = link.source.map((v, i) => `${link.join_id}_join_table.${link.on[i]} = t.${v}`).join(' AND ');
						}
						return `LEFT JOIN ${link.table} ${link.join_id}_join_table
                            on ${joinOn} 
                                and t.${link.link_date} >= ${link.join_id}_join_table.${columnConfig._startdate}
                                and (t.${link.link_date} <= ${link.join_id}_join_table.${columnConfig._enddate} or ${link.join_id}_join_table.${columnConfig._current})`;
					}
				});

				if (sets.length) {
					client.query(`Update ${table} dm
                        SET  ${sets.join(', ')}, ${columnConfig._auditdate} = ${linkAuditdate}
                        FROM ${table} t
                        ${joinTables.join("\n")}
                        where ${nk.map(id => `dm.${id} = t.${id}`).join(' and ')}
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

	client.changeTableStructure = function (structures, callback) {
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
						Object.keys(structures[table].structure).forEach(f => {
							let field = structures[table].structure[f];
							if (!(f in fieldLookup)) {
								missingFields[f] = structures[table].structure[f];
							} else if (field.dimension && !(columnConfig.dimColumnTransform(f, field) in fieldLookup)) {
								let missing_dim = columnConfig.dimColumnTransform(f, field);
								missingFields[missing_dim] = {type: 'integer'};
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

	client.createTable = function (table, definition, callback) {
		let fields = [];
		let dbType = (config.type || "").toLowerCase();
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
					type: 'integer primary key'
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
			fields.push(`"${key}" ${field.type}`);
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
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._deleted} boolean`, done));

			// redshift doesn't support create index
			if (config.version != "redshift") {
				tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${table} using btree(${columnConfig._auditdate})`, done));
				tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.join(',')})`, done));
			}
		}
		queries.map(q => {
			tasks.push(done => client.query(q, err => done(err)));
		});
		async.series(tasks, callback);
	};
	client.updateTable = function (table, definition, callback) {
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
					let dbType = (config.type || "").toLowerCase();
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
			} else if (field.dimension == "d_time" || field.dimension == "time" || field.dimension == "dim_date") {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
				}
			} else if (field.dimension) {
				fields.push(`${columnConfig.dimColumnTransform(key, field)} integer`);
			}
			fields.push(`"${key}" ${field.type}`);
		});
		let sqls = [`alter table  ${table} 
				add column ${fields.join(',\n add column ')}
			`];

		// redshift doesn't support multi 'add column' in one query
		if (config.version == "redshift") {
			sqls = fields.map(field => `alter table  ${table} add column ${field}`);
		}

		queries.map(q => {
			sqls.push(q);
		});
		async.eachSeries(sqls, function (sql, done) {
			client.query(sql, err => done(err));
		}, callback);
	};

	client.findAuditDate = function (table, callback) {
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

	client.exportChanges = function (table, fields, remoteAuditdate, opts, callback) {
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
					var field = fields.map(field => {
						needs[field] = false;
						return `"${field}"`;
					});
					Object.keys(needs).map(key => {
						if (needs[key]) {
							field.push(`"${key}"`);
						}
					});

					if (config.version == "redshift") {
						let fileBase = `s3://${opts.bucket}${opts.file}/${table}`;
						let query = `UNLOAD ('select ${field} from ${client.escapeId(table)} ${where}') to '${fileBase}' MANIFEST OVERWRITE ESCAPE iam_role '${opts.role}';`;
						client.query(query, (err) => {
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

	client.importChanges = function (file, table, fields, opts, callback) {
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
		let qualifiedStagingTable = `${columnConfig.stageSchema}.staging_${tableName}`;
		tasks.push((done) => {
			client.query(`drop table if exists ${qualifiedStagingTable}`, done);
		});
		tasks.push((done) => {
			client.query(`create /*temporary*/ table ${qualifiedStagingTable} (like ${tableName})`, done);
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
				client.query(`copy ${qualifiedStagingTable} (${f})
          from '${file}' ${manifest} ${opts.role ? `credentials 'aws_iam_role=${opts.role}'` : ""}
		  NULL AS '\\\\N' format csv DELIMITER '|' ACCEPTINVCHARS TRUNCATECOLUMNS ACCEPTANYDATE TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' COMPUPDATE OFF`, done);
			} else {
				done("Postgres importChanges Not Implemented Yet");
			}
		});
		if (table.isDimension) {
			tasks.push((done) => {
				client.query(`delete from ${tableName} using ${qualifiedStagingTable} where ${qualifiedStagingTable}.${table.sk}=${tableName}.${table.sk}`, done);
			});
		} else {
			tasks.push((done) => {
				let ids = table.nks.map(nk => `${qualifiedStagingTable}.${nk}=${tableName}.${nk}`).join(' and ');
				if (ids.length) {
					client.query(`delete from ${tableName} using ${qualifiedStagingTable} where ${ids}`, done);
				} else {
					done();
				}
			});
		}
		tasks.push(function (done) {
			client.query(`insert into ${tableName} select * from ${qualifiedStagingTable}`, done);
		});
		tasks.push(function (done) {
			client.query(`select count(*) from ${qualifiedStagingTable}`, (err, result) => {
				loadCount = result && parseInt(result[0].count);
				done(err);
			});
		});
		tasks.push(function (done) {
			client.query(`drop table if exists ${qualifiedStagingTable}`, done);
		});
		async.series(tasks, (err) => {
			callback(err, loadCount);
		});
	};

	return client;
};

'use strict';
const postgres = require('./connect.js');
const async = require('async');
const ls = require('leo-sdk').streams;
const logger = require('leo-logger');
const { basename } = require('path');
const { doesNotThrow } = require('assert');
const { table } = require('console');

module.exports = function (config, columnConfig) {
	let client = postgres(config);
	let dwClient = client;
	let tempTables = [];
	columnConfig = Object.assign({
		_auditdate: '_auditdate',
		_current: '_current',
		_deleted: '_deleted',
		_enddate: '_enddate',
		_startdate: '_startdate',
		dimColumnTransform: (column, field) => {
			field = field || {};
			let dimCol = field[`dim_column${column.replace(field.id, '')}`];
			if (dimCol) {
				return dimCol;
			}
			return field.dim_column ? field.dim_column : `d_${column.replace(/_id$/, '').replace(/^d_/, '')}`;
		},
		stageSchema: 'public',
		stageTablePrefix: 'staging',
		useSurrogateDateKeys: true,
	}, columnConfig || {});


	// Control flow for both of these configurations set to true has not been added. An error will be thrown until that is supported.
	if (
		(config.hashedSurrogateKeys && !config.bypassSlowlyChangingDimensions) // hashed surrogate keys and slowly changing dimensions not supported
		|| (config.hashedSurrogateKeys && config.bypassSlowlyChangingDimensions === undefined) // covering the case where slowly changing dimensions has been omitted
	) {
		logger.error(`Unsupported configuration, bypassSlowlyChangingDimensions:${config.bypassSlowlyChangingDimensions} hashedSurrogateKeys:${config.hashedSurrogateKeys}.`);
		process.exit();
	};

	client.getDimensionColumn = columnConfig.dimColumnTransform;
	client.columnConfig = columnConfig;

	function deletesSetup(qualifiedTable, schema, field, value, where = '') {
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
					return deleteDone => client.query(`update ${qualifiedTable} set ${field} = ${value} where ${col} in (${toDelete[col].join(',')}) ${where}`, deleteDone);
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
			},
		};
	}

	/**
	 * Drop temp tables when we’re finished with them
	 */
	client.dropTempTables = async () => {
		if (tempTables.length) {
			let tasks = [];

			tempTables.forEach(table => {
				tasks.push(done => client.query(`drop table ${table}`, done));
			});

			return new Promise(resolve => {
				async.series(tasks, err => {
					if (err) {
						throw err;
					} else {
						tempTables = [];
						return resolve('Cleaned up temp tables');
					}
				});
			});
		}

		return true;
	};

	client.importFact = function (stream, table, ids, callback) {
		const stagingTable = `${columnConfig.stageTablePrefix}_${table}`;
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

		let sortKey;

		tempTables.push(qualifiedStagingTable);
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		if (config.version !== 'redshift') {
			tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
			tasks.push(done => client.query(`create index ${stagingTable}_id on ${qualifiedStagingTable} (${ids.join(', ')})`, done));
		} else {
			// get sortkey for joins
			tasks.push(done => {
				client.query(`SELECT sortkey
					 		  FROM   public.mv_dist_sort_key
					 		  WHERE  table_name = '${table}';`, (err, results) => {
					if (err) {
						return done(err);
					} else {
						if (results[0].sortKey !== null) {
							sortKey = results[0].sortkey;
						};
						done();
					};
				});
			});
			tasks.push(done =>
				client.query(`CREATE TABLE ${qualifiedStagingTable} 
							  DISTSTYLE ALL 
							  SORTKEY (${sortKey !== undefined ? sortKey : ids[0]})
							  AS SELECT *
								 FROM   ${qualifiedTable}
								 LIMIT  0;`, done));
		};


		tasks.push(done => {
			ls.pipe(stream, ls.through((obj, done, push) => {
				if (obj.__leo_delete__) {
					if (obj.__leo_delete__ === 'id') {
						push(obj);
					}
					deleteHandler.add(obj, done);
				} else {
					done(null, obj);
				}
			}), config.version !== 'redshift' ? client.streamToTable(qualifiedStagingTable) : client.streamToTableFromS3(qualifiedStagingTable, config), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		if (config.version !== 'redshift') {
			tasks.push(done => client.query(`analyze ${qualifiedStagingTable}`, done));
		};

		client.describeTable(table).then(result => {
			let columns = result.filter(field => !field.column_name.match(/^_/)).map(field => `"${field.column_name}"`);

			client.connect().then(connection => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

					let tasks = [];
					let totalRecords = 0;

					// The following code relies on the fact that now()/sysdate will return the same time during all transaction events
					tasks.push(done => connection.query(`Begin Transaction`, done));

					if (!config.hashedSurrogateKeys) {
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
											  where ${ids.map(id => `prev.${id} = staging.${id}`).join(' and ')}`
								, done);
						});

						// Now insert any we were missing
						tasks.push(done => {
							connection.query(`INSERT INTO ${qualifiedTable} (${columns.join(',')},${columnConfig._deleted},${columnConfig._auditdate})
											  SELECT ${columns.map(column => `staging.${column}`)}, false AS ${columnConfig._deleted}, ${dwClient.auditdate} as ${columnConfig._auditdate}
											  FROM ${qualifiedStagingTable} staging
											  WHERE NOT EXISTS ( SELECT *
																 FROM   ${qualifiedTable} as prev
																 WHERE  ${ids.map(id => `prev.${id} = staging.${id}`).join(' and ')})`, done);
						});
					} else {
						columns = columns.concat([columnConfig._auditdate, columnConfig._deleted]);
						let naturalKeyLowerBound;
						let naturalKeyFilter;

						// Get lower bound for natural key to avoid unnecessary scanning
						tasks.push(done => {
							connection.query(`SELECT MIN(${(sortKey !== undefined) ? sortKey : ids[0]}) AS minid,
													 CAST(COUNT(*) AS INT) AS cnt
											  FROM   ${qualifiedStagingTable};`, (err, results) => {
								if (err) {
									return done(err);
								} else if (results[0].cnt === 0) {
									totalRecords = results[0].cnt;
									return done();
								} else {
									totalRecords = results[0].cnt;
									naturalKeyLowerBound = results[0].minid;
									if (naturalKeyLowerBound !== null) {
										naturalKeyFilter = (typeof naturalKeyLowerBound === 'string') ? `'${results[0].minid}'` : `${results[0].minid}`
									};
									done();
								};
							});
						});

						const qualifiedStagingTablePrevious = `${qualifiedStagingTable}_previous`;
						tempTables.push(`${qualifiedStagingTablePrevious}`);
						tasks.push(done => connection.query(`DROP TABLE IF EXISTS ${qualifiedStagingTablePrevious};`, done));
						tasks.push(done => {
							connection.query(`CREATE TABLE ${qualifiedStagingTablePrevious}
											  DISTSTYLE ALL 
											  AS SELECT *
												 FROM   ${qualifiedTable}
												 LIMIT  0;`, done);
						});

						// Set audit date for staged data
						tasks.push(done => {
							connection.query(`UPDATE ${qualifiedStagingTable}
											  SET ${columnConfig._auditdate} = ${dwClient.auditdate};`, done);
						});

						// Retreive copy of existing data
						tasks.push(done => {
							connection.query(`INSERT INTO ${qualifiedStagingTablePrevious} (${columns.map(column => `${column}`).join(`, `)})
											  SELECT ${columns.map(column => `${column}`).join(`, `)}
											  FROM   ${qualifiedTable} AS base
											  WHERE  EXISTS ( SELECT *
															  FROM   ${qualifiedStagingTable} AS staging
															  WHERE  ${ids.map(id => `base.${id} = staging.${id}`).join(` AND `)}
															  		 ${(naturalKeyFilter !== undefined) ? `AND staging.${(sortKey !== undefined) ? sortKey : ids[0]} >= ${naturalKeyFilter}` : ``})
													 ${(naturalKeyFilter !== undefined) ? `AND base.${(sortKey !== undefined) ? sortKey : nk[0]} >= ${naturalKeyFilter}` : ``};`, done);
						});

						// Merge exiting data into staged copy
						tasks.push(done => {
							connection.query(`UPDATE ${qualifiedStagingTable} AS staging
											  SET    ${columns.map(column => `${column} = COALESCE(staging.${column}, prev.${column})`).join(`,`)}
											  FROM   ${qualifiedStagingTablePrevious} AS prev
											  WHERE  ${ids.map(id => `prev.${id} = staging.${id}`).join(` AND `)}`, done);
						});

						// Delete and reinsert data - avoids costly updates on large tables
						tasks.push(done => {
							connection.query(`DELETE FROM ${qualifiedTable}
											  USING  ${qualifiedStagingTable}
											  WHERE  ${ids.map(id => `${qualifiedTable}.${id} = ${qualifiedStagingTable}.${id}`).join(` AND `)}
											  		 ${(naturalKeyFilter !== undefined) ? `AND ${qualifiedTable}.${sortKey !== undefined ? sortKey : ids[0]} >= ${naturalKeyFilter}` : ``}; `, done);
						});
						tasks.push(done => {
							connection.query(`INSERT INTO ${qualifiedTable} (${columns.map(column => `${column}`).join(`, `)})
											  SELECT ${columns.map(column => `${column}`).join(`, `)}
											  FROM   ${qualifiedStagingTable}; `, done);
						});
					};

					async.series(tasks, err => {
						if (!err) {
							connection.query(`commit`, e => {
								connection.release();
								callback(e || err, {
									count: totalRecords,
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
		}).catch(callback);
	};

	client.importDimension = function (stream, table, sk, nk, scds, callback, tableDef = {}) {
		const stagingTbl = `${columnConfig.stageTablePrefix}_${table}`;
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
		schema[qualifiedStagingTable] = schema[qualifiedTable].filter(c => c.column_name !== sk);

		let tasks = [];
		let deleteHandler = deletesSetup(qualifiedTable, schema[qualifiedTable], columnConfig._enddate, dwClient.auditdate, `${columnConfig._current} = true`);

		let sortKey;

		// Prepare staging tables
		tempTables.push(qualifiedStagingTable);
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}`, done));
		tasks.push(done => client.query(`drop table if exists ${qualifiedStagingTable}_changes`, done));
		if (config.version !== 'redshift') {
			tasks.push(done => client.query(`create table ${qualifiedStagingTable} (like ${qualifiedTable})`, done));
			tasks.push(done => client.query(`create index ${stagingTbl}_id on ${qualifiedStagingTable} (${nk.join(', ')})`, done));
		} else {
			// get sortkey for joins
			tasks.push(done => {
				client.query(`SELECT sortkey
					  		  FROM   public.mv_dist_sort_key
					  		  WHERE  table_name = '${table}';`, (err, results) => {
					if (err) {
						return done(err);
					} else {
						if (results[0].sortkey !== null) {
							sortKey = results[0].sortkey;
						};
						done();
					};
				});
			});
			tasks.push(done =>
				// Create staging table with DISTSTYLE ALL to prevent cross talk
				client.query(`CREATE TABLE ${qualifiedStagingTable} 
							  DISTSTYLE ALL
							  SORTKEY(${sortKey !== undefined ? sortKey : nk[0]})
							  AS SELECT *
							  FROM ${qualifiedTable}
								LIMIT 0;`, done));
		};

		// Retain surrogate key column when using hashed keys on redshift // column must be dropped for postgres
		if (!config.hashedSurrogateKeys && config.version !== 'redshift') {
			tasks.push(done => client.query(`alter table ${qualifiedStagingTable} drop column ${sk}`, done));
		};

		tasks.push(done => {
			ls.pipe(stream, ls.through((obj, done, push) => {
				if (obj.__leo_delete__) {
					if (obj.__leo_delete__ === 'id') {
						push(obj);
					}
					deleteHandler.add(obj, done);
				} else {
					done(null, obj);
				}
			}), config.version !== 'redshift' ? client.streamToTable(qualifiedStagingTable) : client.streamToTableFromS3(qualifiedStagingTable, config), (err) => {
				if (err) {
					return done(err);
				} else {
					deleteHandler.flush(done);
				}
			});
		});

		if (config.version !== 'redshift') {
			tasks.push(done => client.query(`analyze ${qualifiedStagingTable}`, done));
		};

		client.describeTable(table).then(result => {
			client.connect().then(connection => {
				async.series(tasks, err => {
					if (err) {
						return callback(err);
					}

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

					if (!config.bypassSlowlyChangingDimensions) {
						if (scd1.length) {
							scdSQL.push(`CASE WHEN md5(${scd1.map(f => 'md5(coalesce(s.' + f + '::text,\'\'))').join(' || ')}) = md5(${scd1.map(f => 'md5(coalesce(d.' + f + '::text,\'\'))').join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD1`);
						} else {
							scdSQL.push(`0 as runSCD1`);
						}
						if (scd2.length) {
							scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 WHEN md5(${scd2.map(f => 'md5(coalesce(s.' + f + '::text,\'\'))').join(' || ')}) = md5(${scd2.map(f => 'md5(coalesce(d.' + f + '::text,\'\'))').join(' || ')}) THEN 0 ELSE 1 END as runSCD2`);
						} else {
							scdSQL.push(`CASE WHEN d.${nk[0]} is null then 1 ELSE 0 END as runSCD2`);
						}
						if (scd3.length) {
							scdSQL.push(`CASE WHEN md5(${scd3.map(f => 'md5(coalesce(s.' + f + '::text,\'\'))').join(' || ')}) = md5(${scd3.map(f => 'md5(coalesce(d.' + f + '::text,\'\'))').join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD3`);
						} else {
							scdSQL.push(`0 as runSCD3`);
						}
						if (scd6.length) {
							scdSQL.push(`CASE WHEN md5(${scd6.map(f => 'md5(coalesce(s.' + f + '::text,\'\'))').join(' || ')}) = md5(${scd6.map(f => 'md5(coalesce(d.' + f + '::text,\'\'))').join(' || ')}) THEN 0 WHEN d.${nk[0]} is null then 0 ELSE 1 END as runSCD6`);
						} else {
							scdSQL.push(`0 as runSCD6`);
						}
					};

					let fields = [sk].concat(allColumns).concat([columnConfig._auditdate, columnConfig._startdate, columnConfig._enddate, columnConfig._current]);
					let tasks = [];
					let totalRecords = 0;

					if (!config.hashedSurrogateKeys && !config.bypassSlowlyChangingDimensions) {
						tempTables.push(`${qualifiedStagingTable}_changes`);
						connection.query(`create table ${qualifiedStagingTable}_changes as
										  select ${nk.map(id => `s.${id}`).join(', ')},
												 d.${nk[0]} is null as isNew
										  ${!config.bypassSlowlyChangingDimensions ? `,${scdSQL.join(',\n')}` : ``}
										  FROM ${qualifiedStagingTable} s
										  LEFT JOIN ${qualifiedTable} d on ${nk.map(id => `d.${id} = s.${id}`).join(' and ')} and d.${columnConfig._current}`, (err) => {
							if (err) {
								logger.error(err);
								process.exit();
							}

							let rowId = null;

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

							// The following code relies on the fact that now()/sysdate will return the same time during all transaction events
							tasks.push(done => connection.query(`Begin Transaction`, done));

							tasks.push(done => {
								connection.query(`INSERT INTO ${qualifiedTable} (${fields.join(',')})
												  SELECT row_number() over() + ${rowId}, ${allColumns.map(column => `coalesce(staging.${column}, prev.${column})`)}, ${dwClient.auditdate} as ${columnConfig._auditdate}, 
														 case when changes.isNew then '1900-01-01 00:00:00' else ${config.version === 'redshift' ? 'sysdate' : 'now()'} END as ${columnConfig._startdate},
														 '9999-01-01 00:00:00' as ${columnConfig._enddate},
														 true as ${columnConfig._current}
												  FROM ${qualifiedStagingTable}_changes changes  
												  JOIN ${qualifiedStagingTable} staging on ${nk.map(id => `staging.${id} = changes.${id}`).join(' and ')}
												  LEFT JOIN ${qualifiedTable} as prev on ${nk.map(id => `prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._current}
												  WHERE (changes.runSCD2 =1 OR changes.runSCD6=1)		
									`, done);
							});

							// This needs to be done last
							tasks.push(done => {
								// RUN SCD1 / SCD6 columns  (where we update the old records)
								let columns = scd1.map(column => `"${column}" = coalesce(staging."${column}", prev."${column}")`).concat(scd6.map(column => `"current_${column}" = coalesce(staging."${column}", prev."${column}")`));
								columns.push(`"${columnConfig._enddate}" = case when changes.runSCD2 =1 then ${config.version === 'redshift' ? 'sysdate' : 'now()'} else prev."${columnConfig._enddate}" END`);
								columns.push(`"${columnConfig._current}" = case when changes.runSCD2 =1 then false else prev."${columnConfig._current}" END`);
								columns.push(`"${columnConfig._auditdate}" = ${dwClient.auditdate}`);
								connection.query(`update ${qualifiedTable} as prev
												  set  ${columns.join(', ')}
												  FROM ${qualifiedStagingTable}_changes changes
												  JOIN ${qualifiedStagingTable} staging on ${nk.map(id => `staging.${id} = changes.${id}`).join(' and ')}
												  where ${nk.map(id => `prev.${id} = changes.${id}`).join(' and ')} and prev.${columnConfig._startdate} != ${config.version === 'redshift' ? 'sysdate' : 'now()'} and changes.isNew = false /*Need to make sure we are only updating the ones not just inserted through SCD2 otherwise we run into issues with multiple rows having .${columnConfig._current}*/
												  and (changes.runSCD1=1 OR  changes.runSCD6=1 OR changes.runSCD2=1)
											`, done);
							});
						});
					} else {
						let naturalKeyLowerBound;
						let naturalKeyFilter;

						// Get lower bound for natural key to avoid unnecessary scanning
						tasks.push(done => {
							connection.query(`SELECT MIN(${(sortKey !== undefined) ? sortKey : nk[0]}) AS minid,
													 CAST(COUNT(*) AS INT) AS cnt
											  FROM   ${qualifiedStagingTable};`, (err, results) => {
								if (err) {
									return done(err);
								} else if (results[0].cnt === 0) {
									totalRecords = results[0].cnt;
									return done();
								} else {
									totalRecords = results[0].cnt;
									naturalKeyLowerBound = results[0].minid;
									if (naturalKeyLowerBound !== null) {
										naturalKeyFilter = (typeof naturalKeyLowerBound === 'string') ? `'${results[0].minid}'` : `${results[0].minid}`;
									};
									done();
								};
							});
						});

						const qualifiedStagingTablePrevious = `${qualifiedStagingTable}_previous`;
						tempTables.push(`${qualifiedStagingTablePrevious}`);
						tasks.push(done => connection.query(`DROP TABLE IF EXISTS ${qualifiedStagingTablePrevious};`, done));
						tasks.push(done => {
							connection.query(`CREATE TABLE ${qualifiedStagingTablePrevious}
											  DISTSTYLE ALL
											  AS SELECT *
											  FROM ${qualifiedTable}
											  LIMIT 0;`, done);
						});

						// Set auditdate and surrogate key for stage data (not sure if the sk is actually necessary)
						tasks.push(done => {
							connection.query(`UPDATE ${qualifiedStagingTable}
											  SET    ${columnConfig._auditdate} = ${dwClient.auditdate},
											  		 ${sk} = farmFingerPrint64(${nk.map(id => `${id}`).join(`|| '-' ||`)}); `, done);
						});

						tasks.push(done => connection.query(`BEGIN TRANSACTION;`, done));

						// Retreive copy of existing data
						tasks.push(done => {
							connection.query(`INSERT INTO ${qualifiedStagingTablePrevious}(${fields.map(column => `${column}`).join(`, `)})
											  SELECT ${fields.map(column => `${column}`).join(`, `)}
											  FROM   ${qualifiedTable} AS base
											  WHERE  EXISTS(SELECT *
											  FROM   ${qualifiedStagingTable} AS staging
															 WHERE  base.${sk} = staging.${sk}
																	${(naturalKeyFilter !== undefined) ? `AND staging.${(sortKey !== undefined) ? sortKey : nk[0]} >= ${naturalKeyFilter}` : ``})
													 ${(naturalKeyFilter !== undefined) ? `AND base.${(sortKey !== undefined) ? sortKey : nk[0]} >= ${naturalKeyFilter}` : ``}; `, done);
						});

						// Merge exiting data into staged copy
						tasks.push(done => {
							connection.query(`UPDATE ${qualifiedStagingTable} AS staging
											  SET    ${fields.map(column => `${column} = COALESCE(staging.${column}, prev.${column})`).join(`, `)}
											  FROM   ${qualifiedStagingTablePrevious} AS prev
											  WHERE  staging.${sk} = prev.${sk}`, done);
						});

						// Delete and reinsert data - avoids costly updates on large tables
						tasks.push(done => {
							connection.query(`DELETE FROM ${qualifiedTable}
											  USING  ${qualifiedStagingTable}
											  WHERE  ${qualifiedTable}.${sk} = ${qualifiedStagingTable}.${sk}
											  		 ${(naturalKeyFilter !== undefined) ? `AND ${qualifiedTable}.${sortKey !== undefined ? sortKey : nk[0]} >= ${naturalKeyFilter}` : ``}; `, done);
						});
						tasks.push(done => {
							connection.query(`INSERT INTO ${qualifiedTable} (${fields.map(column => `${column}`).join(`, `)})
											  SELECT ${fields.map(column => `${column}`).join(`, `)}
											  FROM   ${qualifiedStagingTable}; `, done);
						});
					};
					async.series(tasks, err => {
						if (!err) {
							connection.query(`commit`, e => {
								connection.release();
								callback(e || err, {
									count: totalRecords,
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
		}).catch(callback);
	};

	client.insertMissingDimensions = function (usedTables, tableConfig, tableSks, tableNks, callback) {
		if (config.hashedSurrogateKeys) {
			callback(null);
		} else {
			let unions = {};
			let isDate = {
				d_date: true,
				d_datetime: true,
				d_time: true,
				date: true,
				datetime: true,
				dim_date: true,
				dim_datetime: true,
				dim_time: true,
				time: true,
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
							let _auditdate = dwClient.auditdate;
							let unionQuery = unions[table].join('\nUNION\n');
							transaction.query(`insert into ${table} (${sk}, ${nk}, ${columnConfig._auditdate}, ${columnConfig._startdate}, ${columnConfig._enddate}, ${columnConfig._current}) select row_number() over() + ${rowId}, sub.id, max(${_auditdate})::timestamp, '1900-01-01 00:00:00', '9999-01-01 00:00:00', true from(${unionQuery}) as sub where sub.id is not null group by sub.id`, (err) => {
								done(err);
							});
						});
					}).catch(done);
				};
			});
			async.parallelLimit(missingDimTasks, 10, (missingDimError) => {
				logger.info(`Missing Dimensions ${!missingDimError && 'Inserted'} ----------------------------`, missingDimError || '');
				callback(missingDimError);
			});
		};
	};

	client.linkDimensions = function (table, links, nk, callback, tableStatus) {
		client.describeTable(table).then(() => {
			let tasks = [];
			let sets = [];

			const qualifiedTable = `public.${table}`;
			const linkAuditdate = client.escapeValueNoToLower(new Date().toISOString().replace(/\.\d*Z/, 'Z'));

			// Only run analyze on the table if this is the first load
			if (tableStatus === 'First Load' && config.version !== 'redshift') {
				tasks.push(done => client.query(`ANALYZE ${table}`, done));
			}

			const qualifiedStagingTable = `${columnConfig.stageSchema}.${columnConfig.stageTablePrefix}_${table} `;
			let naturalKeyLowerBound;
			let naturalKeyFilter;
			let sortKey;
			// Get lower bound for natural key to avoid unnecessary scanning
			if (config.version === 'redshift') {
				tasks.push(done => {
					client.query(`SELECT sortkey
								  FROM   public.mv_dist_sort_key
								  WHERE  table_name = '${table}'; `, (err, results) => {
						if (err) {
							return done(err);
						} else {
							if (results[0].sortKey !== null) {
								sortKey = results[0].sortkey;
							};
							done();
						};
					});
				});
				tasks.push(done => {
					client.query(`SELECT MIN(${(sortKey !== undefined) ? sortKey : nk[0]}) AS minid
							  	  FROM   ${qualifiedStagingTable}; `, (err, results) => {
						if (err) {
							return done(err);
						} else {
							naturalKeyLowerBound = results[0].minid;
							if (naturalKeyLowerBound !== null) {
								naturalKeyFilter = (typeof naturalKeyLowerBound === 'string') ? `'${results[0].minid}'` : `${results[0].minid} `
							};
							done();
						};
					});
				});
			};

			tasks.push(done => {
				let joinTables = links.map(link => {
					if (columnConfig.useSurrogateDateKeys && (link.table === 'd_datetime' || link.table === 'datetime' || link.table === 'dim_datetime')) {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else if (columnConfig.useSurrogateDateKeys && (link.table === 'd_date' || link.table === 'date' || link.table === 'dim_date')) {
						sets.push(`${link.destination}_date = coalesce(t.${link.source}::date - '1400-01-01'::date + 10000, 1)`);
					} else if (columnConfig.useSurrogateDateKeys && (link.table === 'd_time' || link.table === 'time' || link.table === 'dim_time')) {
						sets.push(`${link.destination}_time = coalesce(EXTRACT(EPOCH from t.${link.source}::time) + 10000, 1)`);
					} else {
						if (config.hashedSurrogateKeys) {
							sets.push(`${link.destination} = coalesce(farmFingerPrint64(t.${link.source}), 1)`);
							return ``;
						} else {
							sets.push(`${link.destination} = coalesce(${link.join_id}_join_table.${link.sk}, 1)`);
							var joinOn = `${link.join_id}_join_table.${link.on} = t.${link.source} `;

							if (Array.isArray(link.source)) {
								joinOn = link.source.map((v, i) => `${link.join_id}_join_table.${link.on[i]} = t.${v} `).join(' AND ');
							}
							return `LEFT JOIN ${link.table} ${link.join_id}_join_table
									ON ${joinOn} 
									AND t.${link.link_date} >= ${link.join_id}_join_table.${columnConfig._startdate}
									AND(t.${link.link_date} <= ${link.join_id}_join_table.${columnConfig._enddate} or ${link.join_id}_join_table.${columnConfig._current})`;
						};
					}
				});

				if (sets.length) {
					client.query(`UPDATE ${table} dm
                       			  SET  ${sets.join(', ')}, ${columnConfig._auditdate} = ${linkAuditdate}
                        		  FROM ${table} t
								  ${config.hashedSurrogateKeys ? '' : joinTables.join('\n')}
                        		  WHERE ${nk.map(id => `dm.${id} = t.${id}`).join(' AND ')}
								  AND dm.${columnConfig._auditdate} = ${dwClient.auditdate} AND t.${columnConfig._auditdate} = ${dwClient.auditdate}
								  ${(naturalKeyFilter !== undefined) ? `AND t.${(sortKey !== undefined) ? sortKey : nk[0]} >= ${naturalKeyFilter}` : ``} `, done);
				} else {
					done();
				}
			});
			async.series(tasks, err => {
				callback(err);
			});
		}).catch(err => {
			callback(err);
		});
	};

	client.changeTableStructure = async function (structures) {
		let tasks = [];
		let tableResults = {};

		return new Promise(resolve => {
			client.describeTables().then(() => {
				Object.keys(structures).forEach(table => {
					tableResults[table] = 'Unmodified';
					tasks.push(done => {
						client.describeTable(table).then(fields => {
							let fieldLookup = fields.reduce((acc, field) => {
								acc[field.column_name] = field;
								return acc;
							}, {});
							let missingFields = {};
							if (!structures[table].isDimension) {
								structures[table].structure[columnConfig._deleted] = structures[table].structure[columnConfig._deleted] || 'boolean';
							}
							Object.keys(structures[table].structure).forEach(f => {
								let field = structures[table].structure[f];
								if (!(f in fieldLookup)) {
									missingFields[f] = structures[table].structure[f];
								} else if (field.dimension && !(columnConfig.dimColumnTransform(f, field) in fieldLookup)) {
									let missingDim = columnConfig.dimColumnTransform(f, field);
									missingFields[missingDim] = {
										type: 'integer',
									};
								}
							});
							if (Object.keys(missingFields).length) {
								tableResults[table] = 'Modified';
								client.updateTable(table, missingFields).then(() => {
									// success updating table. Move to the next one.
									done();
								});
							} else {
								done();
							}
						}).catch(err => {
							// if the error is that we couldn’t find schema data, attempt to create the table.
							if (err === 'NO_SCHEMA_FOUND') {
								logger.info('Creating table', table);
								client.createTable(table, structures[table]).then(() => {
									tableResults[table] = 'Created';
									logger.info('table created');
									done();
								}).catch(err => {
									throw err;
								});
							} else {
								throw err;
							}
						});
					});
				});

				async.parallelLimit(tasks, 20, (err) => {
					if (err) {
						throw err;
					}

					resolve(tableResults);
				});
			});
		});
	};

	client.createTable = async function (table, definition) {
		let fields = [];
		let defaults = [];
		let dbType = (config.type || '').toLowerCase();
		let defQueries = definition.queries;
		if (defQueries && !Array.isArray(defQueries)) {
			defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version];
		}
		let queries = [].concat(defQueries || []);

		let ids = [];
		Object.keys(definition.structure).forEach(key => {
			let field = definition.structure[key];
			if (field === 'sk') {
				field = {
					sk: true,
					type: 'integer primary key',
				};
			} else if (typeof field === 'string') {
				field = {
					type: field,
				};
			}

			if (field === 'nk' || field.nk) {
				ids.push(key);
			}
			if (field.queries) {
				let defQueries = field.queries;
				if (!Array.isArray(defQueries)) {
					defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version] || [];
				}
				queries = queries.concat(defQueries);
			}

			if (field.dimension === 'd_datetime' || field.dimension === 'datetime' || field.dimension === 'dim_datetime') {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
					defaults.push({
						column: `${columnConfig.dimColumnTransform(key, field)}_date`,
						value: 1,
					});
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
					defaults.push({
						column: `${columnConfig.dimColumnTransform(key, field)}_time`,
						value: 1,
					});
				}
			} else if (field.dimension === 'd_date' || field.dimension === 'date' || field.dimension === 'dim_date') {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
					defaults.push({
						column: `${columnConfig.dimColumnTransform(key, field)}_date`,
						value: 1,
					});
				}
			} else if (field.dimension === 'd_time' || field.dimension === 'time' || field.dimension === 'dim_time') {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
					defaults.push({
						column: `${columnConfig.dimColumnTransform(key, field)}_time`,
						value: 1,
					});
				}
			} else if (field.dimension) {
				fields.push(`${columnConfig.dimColumnTransform(key, field)} integer`);
				defaults.push({
					column: columnConfig.dimColumnTransform(key, field),
					value: 1,
				});
			}
			fields.push(`"${key}" ${field.type}`);
			defaults.push({
				column: key,
				value: field.sk ? 1 : (field.default ? client.escapeValueNoToLower(field.default) : 'null'),
			});
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
			if (config.version !== 'redshift') {
				tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.concat(columnConfig._current).join(',')})`, done));
				tasks.push(done => client.query(`create index ${table}_bk2 on ${table} using btree(${ids.concat(columnConfig._startdate).join(',')})`, done));
				tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${table} using btree(${columnConfig._auditdate})`, done));
			}

			// Add empty row to new dim
			defaults = defaults.concat([{
				column: columnConfig._auditdate,
				value: config.version === 'redshift' ? 'sysdate' : 'now()',
			}, {
				column: columnConfig._startdate,
				value: client.escapeValueNoToLower('1900-01-01 00:00:00'),
			}, {
				column: columnConfig._enddate,
				value: client.escapeValueNoToLower('9999-01-01 00:00:00'),
			}, {
				column: columnConfig._current,
				value: true,
			}]);
			tasks.push(done => client.query(`insert into ${table} (${defaults.map(f => f.column).join(',\n')}) values (${defaults.map(f => f.value || 'null').join(',')})`, done));
		} else {
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._auditdate} timestamp`, done));
			tasks.push(done => client.query(`alter table ${table} add column ${columnConfig._deleted} boolean`, done));

			// redshift doesn't support create index
			if (config.version !== 'redshift') {
				tasks.push(done => client.query(`create index ${table}${columnConfig._auditdate} on ${table} using btree(${columnConfig._auditdate})`, done));
				tasks.push(done => client.query(`create index ${table}_bk on ${table} using btree(${ids.join(',')})`, done));
			}
		}
		queries.map(q => {
			tasks.push(done => client.query(q, err => done(err)));
		});
		return new Promise(resolve => {
			async.series(tasks, err => {
				if (err) {
					throw err;
				}

				resolve();
			});
		});
	};
	client.updateTable = async function (table, definition) {
		let fields = [];
		let queries = [];
		Object.keys(definition).forEach(key => {
			let field = definition[key];
			if (field === 'sk') {
				field = {
					type: 'integer primary key',
				};
			} else if (typeof field === 'string') {
				field = {
					type: field,
				};
			}
			if (field.queries) {
				let defQueries = field.queries;
				if (!Array.isArray(defQueries)) {
					let dbType = (config.type || '').toLowerCase();
					defQueries = defQueries[`${dbType}-${config.version}`] || defQueries[dbType] || defQueries[config.version] || [];
				}
				queries = queries.concat(defQueries);
			}

			if (field.dimension === 'd_datetime' || field.dimension === 'datetime' || field.dimension === 'dim_datetime') {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_time integer`);
				}
			} else if (field.dimension === 'd_date' || field.dimension === 'date' || field.dimension === 'dim_date') {
				if (columnConfig.useSurrogateDateKeys) {
					fields.push(`${columnConfig.dimColumnTransform(key, field)}_date integer`);
				}
			} else if (field.dimension === 'd_time' || field.dimension === 'time' || field.dimension === 'dim_date') {
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
		if (config.version === 'redshift') {
			sqls = fields.map(field => `alter table  ${table} add column ${field}`);
		}

		queries.map(q => {
			sqls.push(q);
		});

		return new Promise(resolve => {
			async.eachSeries(sqls, function (sql, done) {
				client.query(sql, err => done(err));
			}, err => {
				if (err) {
					throw err;
				}

				resolve();
			});
		});
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
						count: count && count[0].count,
					});
				});
			}
		});
	};

	client.exportChanges = function (table, fields, remoteAuditdate, opts, callback) {
		let auditdateCompare = remoteAuditdate.auditdate != null ? `${columnConfig._auditdate} >= ${client.escapeValue(remoteAuditdate.auditdate)}` : `${columnConfig._auditdate} is null`;
		client.query(`select count(*) as count FROM ${client.escapeId(table)} WHERE ${auditdateCompare}`, (err, result) => {
			if (err) {
				return callback(err);
			}

			let where = '';

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
				if (err) {
					callback(err);
				}

				if (result[0].count) {
					let totalCount = parseInt(result[0].count);
					let needs = {
						[columnConfig._auditdate]: true,
						[columnConfig._current]: opts.isDimension,
						[columnConfig._startdate]: opts.isDimension,
						[columnConfig._enddate]: opts.isDimension,

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

					if (config.version === 'redshift') {
						let fileBase = `s3://${opts.bucket}${opts.file}/${table}`;
						let query = `UNLOAD ('select ${field} from ${client.escapeId(table)} ${where}') to '${fileBase}' MANIFEST OVERWRITE ESCAPE iam_role '${opts.role}';`;
						client.query(query, (err) => {
							callback(err, fileBase + '.manifest', totalCount, result[0].oldest);
						});
					} else {
						let file = `${opts.file}/${table}.csv`;
						ls.pipe(client.streamFromTable(table, {
							columns: field,
							delimeter: '|',
							header: false,
							where,
						}), ls.toS3(opts.bucket, file), (err) => {
							err && logger.error('Stream From table Error:', err);
							callback(err, 's3://' + opts.bucket + '/' + file, totalCount, result[0].oldest);
						});
					}
				} else {
					callback(null, null, 0, null);
				}
			});
		});
	};

	client.importChanges = function (file, table, fields, opts, callback) {
		if (typeof opts === 'function') {
			callback = opts;
			opts = {};
		}
		opts = Object.assign({
			role: null,
		}, opts);
		var tableName = table.identifier;
		var tasks = [];
		let loadCount = 0;
		let qualifiedStagingTable = `${columnConfig.stageSchema}.${columnConfig.stageTablePrefix}_${tableName}`;
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
				[columnConfig._enddate]: opts.isDimension,

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
			if (config.version === 'redshift') {
				let manifest = '';
				if (file.match(/\.manifest$/)) {
					manifest = 'MANIFEST';
				}
				client.query(`copy ${qualifiedStagingTable} (${f})
          from '${file}' ${manifest} ${opts.role ? `credentials 'aws_iam_role=${opts.role}'` : ''}
		  NULL AS '\\\\N' format csv DELIMITER '|' ACCEPTINVCHARS TRUNCATECOLUMNS ACCEPTANYDATE TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' COMPUPDATE OFF`, done);
			} else {
				done('Postgres importChanges Not Implemented Yet');
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
const {
	Pool
} = require('pg');
const logger = require("leo-sdk/lib/logger")("connector.sql.postgres");
const moment = require("moment");
const format = require('pg-format');
const async = require('async');

// require("leo-sdk/lib/logger").configure(true);

const copyFrom = require('pg-copy-streams').from;
const csv = require('fast-csv');
// var TIMESTAMP_OID = 1114;

require('pg').types.setTypeParser(1114, (val) => {
	val += "Z";
	console.log(val);
	return moment(val).unix() + "  " + moment(val).utc().format();
});

const ls = require("leo-sdk").streams;
const clients = {};

let queryCount = 0;
module.exports = function(config) {
	const defaultedConfig = Object.assign({
		user: 'root',
		host: 'localhost',
		database: 'test',
		password: 'a',
		port: 5432,
	}, config, {
		max: 15
	});

	const connectionHash = JSON.stringify(defaultedConfig);
	if (!(connectionHash in clients) || typeof clients[connectionHash] === 'undefined') {
		console.log("CREATING NEW POSTGRES CLIENT");
		clients[connectionHash] = create(connectionHash, new Pool(defaultedConfig));
	} else {
		console.log("REUSING POSTGRES CLIENT");
	}

	return clients[connectionHash];
};

function create(hash, pool, parentCache) {
	const connectionHash = hash;
	let cache = {
		schema: Object.assign({}, parentCache && parentCache.schema || {}),
		timestamp: parentCache && parentCache.timestamp || null
	};
	let client = {
		connect: function(opts) {
			opts = opts || {};
			return pool.connect().then(c => {
				return create(connectionHash, c, opts.type == "isolated" ? {} : cache);
			});
		},
		query: function(query, params, callback, opts = {}) {
			if (typeof params == "function") {
				opts = callback;
				callback = params;
				params = [];
			}
			opts = Object.assign({
				inRowMode: false,
				stream: false
			}, opts || {});
			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			pool.query({
				text: query,
				values: params,
				rowMode: opts.inRowMode ? 'array' : undefined
			}, function(err, result) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					if (!opts.allowError) {
						log.error(`Had error #${queryId}`, err, query);
					}
					callback(err);
				} else {
					callback(null, result.rows, result.fields);
				}
			});
		},
		disconnect: function() {
			clients[connectionHash] = undefined;
			return pool.end();
		},
		end: function(callback) {
			clients[connectionHash] = undefined;
			return pool.end(callback);
		},
		release: (destroy) => {
			pool.release && pool.release(destroy);
		},
		describeTable: function(table, callback, tableSchema = 'public') {
			const tblSch = `${tableSchema}.${table}`;
			if (cache.schema[tblSch]) {
				logger.info(`Table "${tblSch}" schema from cache`, cache.timestamp);
				callback(null, cache.schema[tblSch] || []);
			} else {
				this.describeTables((err, schema) => {
					callback(err, schema && schema[tblSch] || []);
				}, tableSchema);
			}
		},
		describeTables: function(callback, tableSchema = 'public') {
			client.query(`SELECT table_name, column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = '${tableSchema}' order by ordinal_position asc`, (err, result) => {
				let schema = {};
				result && result.map(r => {
					const tblSch = `${tableSchema}.${r.table_name}`;
					if (!(tblSch in schema)) {
						schema[tblSch] = [];
					}
					schema[tblSch].push(r);
				});
				cache.schema = schema;
				cache.timestamp = Date.now();
				logger.info("Caching Schema Table", cache.timestamp);
				callback(err, cache.schema);
			});
		},
		getSchemaCache: function() {
			return cache.schema || {};
		},
		streamToTableFromS3: function( /*table, fields, opts*/ ) {
			//opts = Object.assign({}, opts || {});
		},
		stripAllTriggers: function(callback) {
			// see if strip_all_triggers() exists //select to_regproc('strip_all_triggers')
			client.query('SELECT to_regproc(\'strip_all_triggers\');', (err, rows) => {
				if (err) return callback(err);
				const { to_regproc: stripAllTriggersExists } = rows[0];
				if (stripAllTriggersExists) {
					callback();
				} else {
					this.query(`
						CREATE OR REPLACE FUNCTION strip_all_triggers() RETURNS text AS $$ DECLARE
											triggNameRecord RECORD;
									triggTableRecord RECORD;
							BEGIN
									FOR triggNameRecord IN select distinct(trigger_name) from information_schema.triggers where trigger_schema = 'public' LOOP
											SELECT distinct(event_object_table) INTO triggTableRecord from information_schema.triggers where trigger_name = triggNameRecord.trigger_name;
											RAISE NOTICE 'Dropping trigger: % on table: %', triggNameRecord.trigger_name, triggTableRecord.event_object_table;
											EXECUTE 'DROP TRIGGER ' || triggNameRecord.trigger_name || ' ON ' || triggTableRecord.event_object_table || ';';
									END LOOP;

									RETURN 'done';
							END;
							$$ LANGUAGE plpgsql SECURITY DEFINER;
						`, 
					(err) => {
						if (err) return callback(err);
						this.query('SELECT strip_all_triggers();', (err, rows) => {
							if (err) return callback(err);
							const { strip_all_triggers: stripTriggersValue } = rows[0];
							if (stripTriggersValue === 'done') return callback();
							callback(new Error("strip_all_triggers ran unsuccessfully"));
						});
					});
				}
			});
		},
		streamToTableBatch: function(table, opts) {
			opts = Object.assign({
				records: 10000,
				useReplaceInto: false,
				useOnDuplicateUpdate: false,
				ignoreDestinationMissingTable: false
			}, opts || {});
			let pending = null;
			let columns = [];
			let noTable = false;
			let ready = false;
			let total = 0;

			let primaryKey = [];
			let uniqueKeys = {};
			let uniqueLookup = {};

			client.query(`select
					c.column_name,
					pk.constraint_name,
					pk.constraint_type
				from
					information_schema.columns c
				left join(
						select
							kc.column_name,
							kc.constraint_name,
							tc.constraint_type
						from
							information_schema.key_column_usage kc
						join information_schema.table_constraints tc on
							kc.table_name = tc.table_name
							and kc.table_schema = tc.table_schema
							and kc.constraint_name = tc.constraint_name
						where
							tc.table_name = $1
							and tc.table_schema = 'public'
					) pk on
					pk.column_name = c.column_name
				where
					c.table_name = $1
					and c.table_schema = 'public'                    
				union
				select
					a.attname as column_name,
					i.relname as constraint_name,
					case
						when ix.indisprimary then 'PRIMARY KEY'
						when ix.indisunique then 'UNIQUE'
					end as constraint_type 
				from
					pg_class t,
					pg_class i,
					pg_index ix,
					pg_attribute a
				where
					t.oid = ix.indrelid
					and i.oid = ix.indexrelid
					and a.attrelid = t.oid
					and a.attnum = any(ix.indkey)
					and t.relkind = 'r'
					and t.relname = $1
					and (ix.indisunique or ix.indisprimary);
			`, [table], (err, results) => {
				if (results.length === 0) noTable = true;
				columns = results.map(r => {
					if (r.constraint_type) {
						if (r.constraint_type.toLowerCase() == "primary key") {
							primaryKey.push(r.column_name);
						}
						if (r.constraint_type.toLowerCase() == "unique") {
							if (!(r.constraint_name in uniqueKeys)) {
								uniqueKeys[r.constraint_name] = [];
							}
							if (!(r.column_name in uniqueLookup)) {
								uniqueLookup[r.column_name] = [];
							}
							uniqueKeys[r.constraint_name].push(r.column_name);
							uniqueLookup[r.column_name].push(r.constraint_name);
						}
					}
					return r.column_name;
				}).filter((v, i, self) => {
					return self.indexOf(v) === i;
				});
				ready = true;
				if (pending) {
					pending();
				}
			});
			return ls.bufferBackoff((obj, done) => {
				if (!ready) {
					pending = () => {
						done(null, obj, 1, 1);
					};
				} else {
					done(null, obj, 1, 1);
				}
			}, (records, callback) => {
				if (noTable && opts.ignoreDestinationMissingTable) return callback(null, []);
				if (opts.useReplaceInto) {
					console.log("Replace Inserting " + records.length + " records of ", total);
				} else {
					console.log("Inserting " + records.length + " records of ", total);
				}
				total += records.length;
				let primaryKeyLists = [];
				let toRemoveRecords = {};
				Object.keys(uniqueKeys).forEach(constraint => {
					toRemoveRecords[constraint] = [];
				});
				var values = records.map((r, i) => {
					Object.keys(uniqueKeys).forEach(constraint => {
						toRemoveRecords[constraint].push(
							uniqueKeys[constraint].map(f => r.d[f])
						);
					});
					if (opts.onInsert && primaryKey.length) {
						primaryKeyLists.push(primaryKey.map(f => r.d[f]));
					}
					return { action: r.a, data: columns.map(f => r.d[f]) };
				});
				let cmd = 'INSERT INTO %I (%I) VALUES %L';
				if (opts.useOnDuplicateUpdate && primaryKey.length) {
					cmd += ` ON CONFLICT (${primaryKey.join(',')}) DO UPDATE SET ` + columns.map(f => `
						${f} = EXCLUDED.${f}
					`);
				}
				// Dedupe values by PK (take last)
				const latestValues = values.reverse().filter((value, index, self) =>
					index === self.findIndex((v) => { 
						return primaryKey.every(pk => { 
							return v.data[columns.indexOf(pk)] === value.data[columns.indexOf(pk)];
						});
					})
				);
				// Handle DELETE
				const insertValues = latestValues.reduce((accumulator, current)=> {
					return current.action === 'DELETE'
						? accumulator
						: [...accumulator, current.data];
				},[]);
				const deleteValues = latestValues.reduce((accumulator, current)=> {
					return current.action === 'DELETE'
						? [...accumulator, current.data]
						: accumulator;
				},[]);
				const insertQuery = format(cmd, table, columns, insertValues, columns);
				const deleteQuery = format('DELETE FROM %I WHERE ROW(%I) in (%L)', table, columns, deleteValues);
				const sql = ((insertValues.length > 0) ? insertQuery + ';' : '') + ((deleteValues.length > 0)? deleteQuery + ';' : '');

				client.query(sql, function(err, result) {
					if (err) {
						let tasks = [];
						tasks.push(done => client.query("BEGIN", done));
						tasks.push(done => client.query(format("ALTER TABLE %I DISABLE TRIGGER ALL", table), done));
						Object.keys(uniqueKeys).forEach(constraint => {
							tasks.push(done => {
								//Let's delete the offending records and try again
								client.query(format('DELETE FROM %I WHERE ROW(%I) in (%L)', table, uniqueKeys[constraint], toRemoveRecords[constraint]), done);
							});
						});
						async.series(tasks, err => {
							if (err) {
								callback(err);
							} else {
								client.query(insertQuery, function(err) {
									if (err) {
										callback(err);
									} else {
										client.query("COMMIT", err => {
											if (err) {
												console.log(err);
												callback(err);
											} else {
												if (opts.onInsert) {
													opts.onInsert(primaryKeyLists);
												}
												callback(null, []);
											}
										});
									}
								});
							}
						});
					} else {
						if (opts.onInsert) {
							opts.onInsert(primaryKeyLists);
						}
						callback(null, []);
					}
				}, {
					// allowError: true
				});
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(table /*, opts*/ ) {
			const ts = table.split('.');
			let schema = 'public';
			let shortTable = table;
			if (ts.length > 1) {
				schema = ts[0];
				shortTable = ts[1];
			}
					
			// opts = Object.assign({
			// 	records: 10000
			// }, opts || {});
			let columns = [];
			var stream;
			let myClient = null;
			let pending = null;
			pool.connect().then(c => {
				client.describeTable(shortTable, (err, result) => {
					columns = result.map(f => f.column_name);
					myClient = c;
					stream = myClient.query(copyFrom(`COPY ${table} FROM STDIN (format csv, null '\\N', encoding 'utf-8')`));
					stream.on("error", function(err) {
						console.log(err);
						process.exit();
					});
					if (pending) {
						pending();
					}
				}, schema);
			}, err => {
				console.log(err);
			});

			let count = 0;

			function nonNull(v) {
				if (v === "" || v === null || v === undefined) {
					return "\\N";
				} else {
					return v;
				}
			}

			return ls.pipeline(csv.createWriteStream({
				headers: false,
				transform: (row, done) => {
					if (!myClient) {
						pending = () => {
							done(null, columns.map(f => nonNull(row[f])));
						};
					} else {
						done(null, columns.map(f => nonNull(row[f])));
					}
				}
			}), ls.write((r, done) => {
				count++;
				if (count % 10000 == 0) {
					console.log(table + ": " + count);
				}
				if (!stream.write(r)) {
					stream.once('drain', done);
				} else {
					done(null);
				}
			}, (done, push) => {
				stream.on('end', () => {
					myClient.release(true);
					done();
				});
				stream.end();
			}));
		},
		range: function(table, id, opts, callback) {
			if (Array.isArray(id)) {
				let r = {
					min: {},
					max: {},
					total: 0
				};

				let tasks = [];
				tasks.push(done => {
					client.query(`select count(*) as count from ${table}`, (err, result) => {
						if (!err) {
							r.total = result[0].count;
						}
						done(err);
					});
				});
				tasks.push(done => {
					client.query(`select ${id[0]}, ${id[1]} from ${table} order by ${id[0]} asc, ${id[1]} asc limit 1`, (err, result) => {
						if (!err) {

							r.min = {
								[id[0]]: result[0][id[0]],
								[id[1]]: result[0][id[1]]
							};
						}
						done(err);
					});
				});
				tasks.push(done => {
					client.query(`select ${id[0]}, ${id[1]} from ${table} order by ${id[0]} desc, ${id[1]} desc limit 1`, (err, result) => {
						if (!err) {
							r.max = {
								[id[0]]: result[0][id[0]],
								[id[1]]: result[0][id[1]]
							};
						}
						done(err);
					});
				});
				async.parallel(tasks, (err) => {
					callback(err, r);
				});
			} else {
				client.query(`select min(${id}) as min, max(${id}) as max, count(${id}) as total from ${table}`, (err, result) => {
					if (err) return callback(err);
					callback(null, {
						min: result[0].min,
						max: result[0].max,
						total: result[0].total
					});
				});
			}
		},
		nibble: function(table, id, start, min, max, limit, reverse, callback) {
			let sql;
			if (Array.isArray(id)) {
				if (reverse) {
					sql = `select ${id[0]}, ${id[1]} from ${table}  
							where (${id[0]} = ${start[id[0]]} and ${id[1]} <= ${start[id[1]]}) 
									OR
								  ${id[0]} < ${start[id[0]]}
							ORDER BY ${id[0]} desc, ${id[1]} desc
							LIMIT 2 OFFSET ${limit-1}`;
				} else {
					sql = `select ${id[0]}, ${id[1]} from ${table}  
							where (${id[0]} = ${start[id[0]]} and ${id[1]} >= ${start[id[1]]}) 
									OR
								  ${id[0]} > ${start[id[0]]}
							ORDER BY ${id[0]} asc, ${id[1]} asc
							LIMIT 2 OFFSET ${limit-1}`;
				}
				client.query(sql, (err, result) => {
					let r = [];
					if (!err) {
						if (result[0]) {
							r[0] = {
								id: {
									[id[0]]: result[0][id[0]],
									[id[1]]: result[0][id[1]]
								}
							};
						}
						if (result[1]) {
							r[1] = {
								id: {
									[id[0]]: result[1][id[0]],
									[id[1]]: result[1][id[1]]
								}
							};
						}
					}
					callback(err, r);
				});
			} else {
				if (reverse) {
					sql = `select ${id} as id from ${table}  
							where ${id} <= ${start} and ${id} >= ${min}
							ORDER BY ${id} desc
							LIMIT 2 OFFSET ${limit-1}`;
				} else {
					sql = `select ${id} as id from ${table}  
							where ${id} >= ${start} and ${id} <= ${max}
							ORDER BY ${id} asc
							LIMIT 2 OFFSET ${limit-1}`;
				}
				client.query(sql, callback);
			}
		},
		getIds: function(table, id, start, end, reverse, callback) {
			if (Array.isArray(id)) {
				let joinTable = '';
				if (reverse) {
					joinTable = `select ${id[0]}, ${id[1]} 
						from ${table} 
						where ((${id[0]} = ${start[id[0]]} and ${id[1]} <= ${start[id[1]]}) OR ${id[0]} < ${start[id[0]]}) 
					      and ((${id[0]} = ${end[id[0]]}   and ${id[1]} >= ${end[id[1]]})   OR ${id[0]} > ${end[id[0]]})
					    order by ${id[0]} asc, ${id[1]} asc`;
				} else {
					joinTable = `select ${id[0]}, ${id[1]} 
						from ${table} 
						where ((${id[0]} = ${start[id[0]]} and ${id[1]} >= ${start[id[1]]}) OR ${id[0]} > ${start[id[0]]}) 
					      and ((${id[0]} = ${end[id[0]]}   and ${id[1]} <= ${end[id[1]]})   OR ${id[0]} < ${end[id[0]]})
					    order by ${id[0]} asc, ${id[1]} asc`;
				}
				callback(null, joinTable);
			} else {
				let sql;
				if (reverse) {
					sql = `select ${id} as id from ${table}  
						where ${id} <= ${start} and ${id} >= ${end}
						ORDER BY ${id} desc`;
				} else {
					sql = `select ${id} as id from ${table}  
						where ${id} >= ${start} and ${id} <= ${end}
						ORDER BY ${id} asc`;
				}
				client.query(sql, callback);
			}
		}
	};
	return client;
}

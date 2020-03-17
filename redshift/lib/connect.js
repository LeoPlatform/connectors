const {
	Pool
} = require('pg');
const logger = require('leo-logger')('connector.sql.postgres');
const moment = require("moment");
const format = require('pg-format');
const async = require('async');

var copyFrom = require('pg-copy-streams').from;
var copyTo = require('pg-copy-streams').to;
let csv = require('fast-csv');
// var TIMESTAMP_OID = 1114;

require('pg').types.setTypeParser(1114, (val) => {
	val += "Z";
	console.log(val);
	return moment(val).unix() + "  " + moment(val).utc().format();
});

const ls = require("leo-sdk").streams;

let queryCount = 0;
module.exports = function(config) {
	const pool = new Pool(Object.assign({
		user: 'root',
		host: 'localhost',
		database: 'test',
		password: 'a',
		port: 5432,
	}, config, {
		max: 15
	}));

	return create(pool);
};

function create(pool, parentCache) {
	let cache = {
		schema: Object.assign({}, parentCache && parentCache.schema || {}),
		timestamp: parentCache && parentCache.timestamp || null
	};
	let client = {
		connect: function(opts) {
			opts = opts || {};
			return pool.connect().then(c => {
				return create(c, opts.type == "isolated" ? {} : cache);
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
					if (opts && opts.returnResultObject) {
						callback(null, result);
					} else {
						callback(null, result.rows, result.fields);
					}
				}
			});
		},
		disconnect: pool.end.bind(pool),
		end: pool.end.bind(pool),
		release: (destroy) => {
			pool.release && pool.release(destroy);
		},
		describeTable: function(table, callback, tableSchema = 'public') {
			const tblSch = `${tableSchema}.${table}`;
			if (cache.schema[tblSch]) {
				logger.info(`Table "${tblSch}" schema from cache`, cache.timestamp);
				callback(null, cache.schema[tblSch] || []);
			} else {
				this.clearSchemaCache();
				this.describeTables((err, schema) => {
					callback(err, schema && schema[tblSch] || []);
				}, tableSchema);
			}
		},
		describeTables: function(callback, tableSchema = 'public') {
			if (Object.keys(cache.schema || {}).length) {
				logger.info(`Tables schema from cache`, cache.timestamp);
				return callback(null, cache.schema);
			}
			client.query(`SELECT table_name, column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = '${tableSchema}' order by ordinal_position asc`, (err, result) => {
				let schema = {};
				result && result.map(r => {
					const tblSch = `${tableSchema}.${r.table_name}`;
					if (!(tblSch in schema)) {
						schema[tblSch] = [];
					}
					schema[tblSch].push(r);
				});
				Object.keys(schema).map((t)=>{
					let parts = t.match(/^public\.(.*)$/);
					if (parts) {
						schema[parts[1]] = schema[t];
					}
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
		setSchemaCache: function(schema) {
			cache.schema = schema || {};
		},
		clearSchemaCache: function() {
			logger.info(`Clearing Tables schema cache`);
			cache.schema = {};
		},
		streamToTableFromS3: function( /*table, fields, opts*/ ) {
			//opts = Object.assign({}, opts || {});
		},
		streamToTableBatch: function(table, opts) {
			opts = Object.assign({
				records: 10000
			}, opts || {});
			opts = Object.assign({
				records: 10000,
				useReplaceInto: false,
				useOnDuplicateUpdate: false
			}, opts || {});
			let pending = null;
			let columns = [];
			let ready = false;
			let total = 0;

			let primaryKey = [];
			let uniqueKeys = {};
			let uniqueLookup = {};


			client.query(`select c.column_name, pk.constraint_name, pk.constraint_type
					FROM information_schema.columns c
					LEFT JOIN (SELECT kc.column_name, kc.constraint_name, tc.constraint_type
  						FROM information_schema.key_column_usage kc
  						JOIN information_schema.table_constraints tc on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
  						WHERE tc.table_name = $1
    						and tc.table_schema = 'public'
						) pk on pk.column_name = c.column_name
					WHERE 
   					c.table_name = $1 and c.table_schema = 'public';
			`, [table], (err, results) => {
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
				var values = records.map((r) => {
					Object.keys(uniqueKeys).forEach(constraint => {
						toRemoveRecords[constraint].push(
							uniqueKeys[constraint].map(f => r[f])
						);
					});
					if (opts.onInsert && primaryKey.length) {
						primaryKeyLists.push(primaryKey.map(f => r[f]));
					}
					return columns.map(f => r[f]);
				});
				let cmd = 'INSERT INTO %I (%I) VALUES %L';
				if (opts.useOnDuplicateUpdate && primaryKey.length) {
					cmd += ` ON CONFLICT (${primaryKey.join(',')}) DO UPDATE SET ` + columns.map(f => `
						${f} = EXCLUDED.${f}
					`);
				}
				let insertQuery = format(cmd, table, columns, values, columns);
				client.query(insertQuery, function(err) {
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
						console.log(`COPY error: ${err.where}`, err);
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
				} else if (typeof v === "string" && v.search(/\r/) !== -1) {
					return v.replace(/\r\n?/g, "\n");
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
			}, (done) => {
				stream.on('end', () => {
					myClient.release(true);
					done();
				});
				stream.end();
			}));
		},
		streamFromTable: function(table, opts) {

			function clean(v) {
				let i = v.search(/(\r|\n)/);
				if (i !== -1 && i < v.length - 1) {
					// Escape any newlines in the middle of the row
					return v.substr(0, v.length - 1).replace(/\n/g, `\\n`) + v[v.length - 1];
				}
				return v;
			}

			opts = Object.assign({
				headers: false,
				delimiter: "|",
				null: "\\N",
				encoding: "utf-8",
				format: "csv",
				columns: null,
				where: null
			}, opts);

			//let columns = [];
			var stream;
			let count = 0;
			let pass = ls.through((row, done) => {
				count++;
				if (count % 10000 == 0) {
					console.log(table + ": " + count);
				}
				done(null, clean(row.toString()));
			});
			let myClient = null;
			pool.connect().then(c => {
				//client.describeTable(table, (err, result) => {
				//	columns = result.map(f => f.column_name);
				myClient = c;
				let columnsList = "";
				if (Array.isArray(opts.columns)) {
					columnsList = opts.columns.join(",");
				} else if (typeof opts.columns === "string") {
					columnsList = opts.columns;
				}
				let query = `COPY ${opts.where ? `(select ${columnsList ? columnsList : "*"} from ${table} ${opts.where})` : `${table} (${columnsList})`} TO STDOUT (format ${opts.format}, null '${opts.null}', encoding '${opts.encoding}', DELIMITER '${opts.delimiter}', HEADER ${opts.headers})`;

				logger.log(query);
				stream = myClient.query(copyTo(query));
				stream.on("error", function(err) {
					console.log(err);
				});
				stream.on("end", function() {
					console.log("Copy stream ended", table);
					myClient.release(true);
				});
				ls.pipe(stream, pass);
				//});
			}, err => {
				console.log(err);
				pass.emit("error", err);
			});
			return pass;
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
		},
		escapeId: function(field) {
			return '"' + field.replace('"', '').replace(/\.([^.]+)$/, '"."$1') + '"';
		},
		escape: function(value) {
			if (value.replace) {
				return '"' + value.replace('"', '') + '"';
			} else {
				return value;
			}
		},
		escapeValue: function(value) {
			if (value.replace) {
				return "'" + value.replace("'", "\\'").toLowerCase() + "'";
			} else {
				return value;
			}
		},
		escapeValueNoToLower: function(value) {
			if (value.replace) {
				return "'" + value.replace("'", "\\'") + "'";
			} else {
				return value;
			}
		}
	};
	return client;
}

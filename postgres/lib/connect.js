const {
	Pool
} = require('pg');
const logger = require("leo-sdk/lib/logger")("connector.sql.postgres");
const moment = require("moment");
const format = require('pg-format');

// require("leo-sdk/lib/logger").configure(true);

var copyFrom = require('pg-copy-streams').from;
let csv = require('fast-csv');
// var TIMESTAMP_OID = 1114;

require('pg').types.setTypeParser(1114, (val) => {
	val += "Z";
	console.log(val);
	return moment(val).unix() + "  " + moment(val).utc().format();
});


let ls = require("leo-sdk").streams;

let queryCount = 0;
module.exports = function(config) {
	const pool = new Pool(Object.assign({
		user: 'root',
		host: 'localhost',
		database: 'test',
		password: 'a',
		port: 5432,
	}, config));

	return create(pool);
};

function create(pool) {
	let client = {
		connect: function() {
			return pool.connect().then(c => {
				return create(c);
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
					log.error(`Had error #${queryId}`, err, query);
					callback(err);
				} else {
					callback(null, result.rows, result.fields);
				}
			});
		},
		disconnect: pool.end.bind(pool),
		end: pool.end.bind(pool),
		release: pool.release && pool.release.bind(pool),
		describeTable: function(table, callback) {
			client.query("SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
				callback(err, result);
			});
		},
		streamToTableFromS3: function( /*table, fields, opts*/ ) {
			//opts = Object.assign({}, opts || {});
		},
		streamToTableBatch: function(table, fields, opts) {
			opts = Object.assign({
				records: 10000
			}, opts || {});
			let fieldColumnLookup = fields.reduce((lookups, f, index) => {
				lookups[f.toLowerCase()] = index;
				return lookups;
			}, {});
			let columns = Object.keys(fieldColumnLookup);
			return ls.bufferBackoff((obj, done) => {
				done(null, obj, 1, 1);
			}, (records, callback) => {
				console.log("Inserting " + records.length + " records");
				var values = records.map((r) => {
					return columns.map(f => r[f]);
				});
				client.query(format('INSERT INTO %I (%I) VALUES %L', table, fields, values), function(err) {
					if (err) {
						callback(err);
					} else {
						callback(null, []);
					}
				});
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(table /*, opts*/ ) {
			// opts = Object.assign({
			// 	records: 10000
			// }, opts || {});
			let columns = [];
			var stream;
			let myClient = null;
			let pending = null;
			pool.connect().then(c => {
				client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
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
				});
			}, err => {
				console.log(err);
			});

			let count = 0;

			function nonNull(v) {
				if (v === null || v === undefined) {
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
					myClient.end();
					done();
				});
				stream.end();
			}));
		},
		range: function(table, id, opts, callback) {
			client.query(`select min(${id}) as min, max(${id}) as max, count(${id}) as total from ${table}`, (err, result) => {
				if (err) return callback(err);
				callback(null, {
					min: result[0].min,
					max: result[0].max,
					total: result[0].total
				});
			});
		},
		nibble: function(table, id, start, min, max, limit, reverse, callback) {
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
		},
		getIds: function(table, id, start, end, reverse, callback) {
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
	};
	return client;
};

const {
	Pool,
	Client
} = require('pg')
const logger = require("leo-sdk/lib/logger")("connector.sql.postgres");

const format = require('pg-format');


var copyFrom = require('pg-copy-streams').from;
let csv = require('fast-csv');
const PassThrough = require("stream").PassThrough;
// var TIMESTAMP_OID = 1114;

require('pg').types.setTypeParser(1114, (val) => {
	val += "Z";
	console.log(val);
	return moment(val).unix() + "  " + moment(val).utc().format();
});


let ls = require("leo-sdk").streams;

module.exports = function(config) {
	const pool = new Pool(Object.assign({
		user: 'root',
		host: 'localhost',
		database: 'test',
		password: 'a',
		port: 5432,
	}, config));

	let queryCount = 0;
	let client = {
		query: function(query, params, callback) {
			if (!callback) {
				callback = params;
				params = null;
			}

			let queryId = ++queryCount;
			let log = logger.sub("query");
			log.info(`SQL query #${queryId} is `, query);
			log.time(`Ran Query #${queryId}`);
			pool.query(query, params, function(err, result) {
				log.timeEnd(`Ran Query #${queryId}`);
				if (err) {
					log.info("Had error", err);
					callback(err);
				} else {
					callback(null, result.rows, result.fields);
				}
			})
		},
		disconnect: pool.end.bind(pool),
		loadFromS3: function(table, fields, opts) {

		},
		streamToTableBatch: function(table, fields, opts) {
			opts = Object.assign({
				records: 10000
			});
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
				})
			}, {
				failAfter: 2
			}, {
				records: opts.records
			});
		},
		streamToTable: function(table, fields, opts) {
			opts = Object.assign({
				records: 10000
			});
			let columns = [];
			var stream;
			let myClient = null;
			let pending = null;
			pool.connect().then(c => {
				client.query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 order by ordinal_position asc", [table], (err, result) => {
					columns = result.map(f => f.column_name);
					myClient = c;

					stream = myClient.query(copyFrom(`COPY ${table} FROM STDIN`));
					stream.on('end', () => {
						myClient.end();
					});
					if (pending) {
						pending();
					}
				});
			}, err => {
				console.log(err);
			});

			let count = 0;
			let pass = new PassThrough();

			function nonNull(v) {
				if (v === null || v === undefined) {
					return "\\N";
				} else {
					return v;
				}
			}

			return ls.pipeline(csv.createWriteStream({
				headers: false,
				delimiter: '\t',
				transform: (row, done) => {
					if (!myClient) {
						pending = () => {
							done(null, columns.map(f => nonNull(row[f])));
						}
					} else {
						done(null, columns.map(f => nonNull(row[f])));
					}
				}
			}), ls.through((r, done) => {
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
				stream.end();
			}));
		}
	};
	return client;
};
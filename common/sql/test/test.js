require("chai").should();

const logger = require('leo-logger')('connector.sql');
const PassThrough = require("stream").PassThrough;
const async = require("async");
const ls = require("leo-sdk").streams;
const mysql = require("mysql");

const sqlLoader = require("../loader");

describe.only('SQL', function() {
	it('Should be able to stream changed IDs in and receive full objects out', function(done) {
		this.timeout(1000 * 10);
		let stream = new PassThrough({
			objectMode: true
		});

		let count = 0;
		const MAX = 24531;
		async.doWhilst((done) => {
			if (!stream.write({
					test: [++count, ++count, ++count]
				})) {
				stream.once('drain', done);
			} else {
				done();
			}
		}, () => count < MAX, (err) => {
			stream.end();
		});

		let m;
		let query;

		let sqlTransform = sqlLoader(
			() => {
				m = mysql.createPool(Object.assign({
					host: "localhost",
					user: "root",
					port: 3306,
					password: "a",
					connectionLimit: 10
				}));
				let queryCount = 0;
				return {
					query: function(query, callback) {
						let queryId = ++queryCount;
						let log = logger.sub("query");
						log.info(`SQL query #${queryId} is `, query);
						log.time(`Ran Query #${queryId}`);
						m.query(query, function(err, result, fields) {
							log.timeEnd(`Ran Query #${queryId}`);
							if (err) {
								log.error("Had error", err);
							}
							callback(err, result, fields);
						});
					},
					disconnect: m.end
				};
			}, {
				test: true
			},
			function(ids) {
				return {
					sql: `select * from test where id in (${ids.join()})`,
					id: "id",
					joins: {
						Customer: {
							type: 'one_to_many',
							on: "id",
							sql: `select * from test where id in (${ids.join()})`
						},
						Bob: {
							type: 'one_to_one',
							on: 'changed',
							sql: `select * from test where id in (${ids.join()})`,
							transform: row => {
								return {
									changed: row.id,
									combined: row.name + "-" + row.somethingelse
								};
							}
						}
					}
				};
			}

		);
		ls.pipe(stream, sqlTransform, ls.log(), ls.devnull(), (err) => {
			console.log("all done in test", err);
			done();
		});


	});
});

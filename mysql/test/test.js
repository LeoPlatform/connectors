require("chai").should();

const logger = require("leo-sdk/lib/logger")("connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");

const mysqlLoader = require("../");

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



		let transform = mysqlLoader({
			host: "localhost",
			user: "root",
			port: 3306,
			database: "datawarehouse",
			password: "a",
			connectionLimit: 10
		}, {
			test: true
		}, function(ids) {
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
		});
		ls.pipe(stream, transform, ls.log(), ls.devnull(), (err) => {
			console.log("all done");
			console.log(err);
			done(err);
		});
	});
	it.only("Should be able to stream the entire table", function(done) {
		this.timeout(1000 * 60 * 4);



		ls.pipe(leo.read("TEST", "Community", {
			// start: 'z/',
			start: "_snapshot/z/2018/03/06/05/11/1520313154185-0267168"
		}), ls.devnull(), err => {
			console.log(err);
			done();
		});
		return;

		//These variables shoud persist between lambda invocations
		let event = "Community";
		let botId = "Community_snapshotter";
		let timestamp = moment();
		//END

		let stream = mysqlLoader.nibble({
			host: "localhost",
			user: "root",
			port: 3306,
			database: "datawarehouse",
			password: "a",
			connectionLimit: 10
		}, "d_community", "d_id");

		let transform = mysqlLoader.load({
			host: "localhost",
			user: "root",
			port: 3306,
			database: "datawarehouse",
			password: "a",
			connectionLimit: 10
		}, {
			d_community: true
		}, function(ids) {
			return {
				sql: `select * from d_community where d_id in (${ids.join()})`,
				id: "d_id"
			};
		});
		ls.pipe(stream, transform, ls.through((obj, done) => {
			done(null, {
				id: botId,
				payload: obj,
				checkpoint: obj.d_id,
				event: event
			});
		}), ls.toS3GzipChunks(event, {
			useS3Mode: true,
			time: {
				minutes: 1
			},
			prefix: "_snapshot/" + timestamp.format("YYYY/MM_DD_") + timestamp.valueOf()
		}, function(done, push) {
			push({
				_cmd: 'registerSnapshot',
				event: event,
				start: timestamp.valueOf(),
				next: timestamp.clone().startOf('day').valueOf()
			});
			done();
		}), ls.toLeo(botId, {
			snapshot: timestamp.valueOf()
		}), (err) => {
			console.log("all done");
			console.log(err);
			done(err);
		});
	});
});

require("chai").should();

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


		//These variables shoud persist between lambda invocations
		let event = "Community";

		let botId = "Community_snapshotter";
		let timestamp = moment();
		let streamContinuation = 'z/' + timestamp.format("YYYY/MM/DD");
		let batch = timestamp.format("YYYY/MM/DD_") + timestamp.valueOf();
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
			snapShot: true,
			time: {
				minutes: 1
			},
			prefix: batch
		}, function(done, push) {
			push({
				_cmd: 'registerSnapshot',
				event: event,
				start: batch,
				end: batch + "0",
				continueFrom: streamContinuation
			});
			done();
		}), ls.toLeo(botId, {
			snapshot: "z/" + batch
		}), (err) => {
			console.log("all done");
			console.log(err);
			done(err);
		});
	});
});

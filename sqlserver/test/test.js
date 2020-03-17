require("chai").should();

const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");

const loader = require("../");
const streamer = require("../").streamChanges;

describe('SQL', function() {
	it('Should be able to stream changed IDs in and receive full objects out', function(done) {
		this.timeout(1000 * 5);

		let changes = streamer({
			user: 'root',
			password: 'Leo1234TestPassword',
			server: 'sampleloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
			database: 'test',
			name: 'streamer'
		}, ['test', 'test']);

		let transform = loader({
			user: 'root',
			password: 'Leo1234TestPassword',
			server: 'sampleloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
			database: 'test',
			name: 'loader'
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

		// this is good
		ls.pipe(changes, transform, ls.log(), ls.devnull(), (err) => {
			console.log("all done");
			console.log(err);
			done(err);
		});
	});
	it.only("Should be able to stream the entire table", function(done) {
		this.timeout(240000);

		let event = 'Lead',
			botId = 'Lead_snapshotter',
			timestamp = moment(),
			// create the stream
			stream = loader.nibble({
				user: 'root',
				password: 'Leo1234TestPassword',
				server: 'sampleloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'rentdynamics'
			}, 'Lead', 'ID', {
				limit: 5000,
				maxLimit: 5000
			}),
			// transform the data
			transform = loader.load({
				user: 'root',
				password: 'Leo1234TestPassword',
				server: 'sampleloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'rentdynamics'
			}, {
				Lead: true
			}, function(ids) {
				return {
					sql: `SELECT * FROM Lead WHERE ID in (${ids.join()})`,
					id: "ID"
				};
			});

		ls.pipe(stream, transform, ls.through((obj, done) => {
			done(null, {
				id: botId,
				payload: obj,
				checkpoint: obj.ID,
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
			console.log('all done');
			console.log(err);
			done(err);
		});

		// for reading the data from S3
		// ls.pipe(leo.read('TEST', 'Community', {
		// 	start: 'z/' // select a starting snapshot, or simply z/ for "beginning of time"
		// }), ls.devnull(), err => {
		// 	console.log(err);
		// 	done();
		// });
	});
});

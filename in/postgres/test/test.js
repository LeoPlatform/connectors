require("chai").should();

const logger = require("leo-sdk/lib/logger")("connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const ls = require("leo-sdk").streams;

const loader = require("../");

describe.only('SQL', function() {
	it('Should be able to stream changed IDs in and receive full objects out', function(done) {
		this.timeout(1000 * 30);
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



		let transform = loader({
			user: 'root',
			host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
			database: 'sourcedata',
			password: 'Leo1234TestPassword',
			port: 5432,
		}, {
			test: true
		}, function(ids) {
			return {
				sql: `select * from test where id in (${ids.join()})`,
				id: "id",
				hasMany: {
					Customer2: {
						on: "changed",
						sql: `select * from test where id in (${ids.join()})`,
						transform: row => {
							return {
								changed: row.id,
								combined: row.name + "-" + row.somethingelse
							};
						}
					}
				},
				hasOne: {
					Bob2: {
						type: {},
						on: "id",
						sql: `select * from test where id in (${ids.join()})`
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
});
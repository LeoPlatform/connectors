require("chai").should();

const logger = require("leo-sdk/lib/logger")("connector.sql");
const async = require("async");
const ls = require("leo-sdk").streams;

const loader = require("../").load;
const streamer = require("../").streamChanges;

describe.only('SQL', function() {
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
		ls.pipe(changes, transform, ls.log(), ls.devnull(), (err) => {
			console.log("all done");
			console.log(err);
			done(err);
		});
	});
});
require("chai").should();
const loader = require("../");

describe('SQL', function() {
	it('Should be binlog read from postgres', function(done) {
		this.timeout(1000 * 500);


		loader.binlogReader({
			user: 'dan',
			host: 'lion.cwcoot5q9ili.us-west-2.rds.amazonaws.com',
			database: 'postgres',
			password: 'None1234',
			port: 5432
		}, "regression_slot");
	});
});
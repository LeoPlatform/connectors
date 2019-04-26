// const { assert } = require('chai');
const dwconnect = require('../../../lib/dwconnect');
const tableConfig = require('./tableConfig');

describe('dwconnect', function() {
	it('Should Create DB from tableconfig', function(done) {
		const client = dwconnect({ user: 'postgres',
			password: 'mytestpassword',
			host: 'localhost',
			port: 5432,
			database: 'postgres',
			username: 'postgres' 
		});
		client.auditdate = client.escapeValueNoToLower(new Date().toISOString().replace(/\.\d*Z/, 'Z'));
		client.changeTableStructure(tableConfig, (err) => {

			// client.query("Select * from dim_foo", (res) => {
			// console.log("RES", res);
			// console.log("CAAAAAAAAALLLLLLLLLLIIIIIIIIIINNNNNNNNNNGGGGGGGGGGG END");
			client.end();
			done(err);
			// });

		});
	});
});

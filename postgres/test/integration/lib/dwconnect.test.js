// const { assert } = require('chai');
const dwconnect = require('../../../lib/dwconnect');
const load = require('leo-connector-common/datawarehouse/load');

const tableConfig = require('./tableConfig');
const mockDwLoadQueueStream = require('./mockDwLoadQueueStream');

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
			//TODO: assert that each table in tableConfig is as expected
			client.end();
			done(err);
		});
	});
	it('Should load some facts an dimensions', function(done) {
		const client = dwconnect({ user: 'postgres',
			password: 'mytestpassword',
			host: 'localhost',
			port: 5432,
			database: 'postgres',
			username: 'postgres' 
		});
		client.auditdate = client.escapeValueNoToLower(new Date().toISOString().replace(/\.\d*Z/, 'Z'));
		client.changeTableStructure(tableConfig, (err) => {
			if (err) return done(err);
			load(client, tableConfig,
				mockDwLoadQueueStream,
				err => {
					console.log("DONE WITH TEST");
					if (err) {
						console.log(err);
						client.disconnect();
						return done(err);
					}
					client.end();
					done();
				});
		});
	});
});

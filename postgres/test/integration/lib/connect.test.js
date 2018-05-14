const connect = require('../../../lib/connect');

describe.skip('PostgreSQL Lib', () => {
	describe('connect', () => {
		it('binLogReader returns a pipable stream', (done)  => {
			const connector = connect({
				user: 'joseph',
				host: 'joseph.cwcoot5q9ili.us-west-2.rds.amazonaws.com',
				database: 'postgres',
				password: 'None1234',
				port: 5432
			});

			connector.stripAllTriggers((err) => {
				done(); //DROP FUNCTION public.strip_all_triggers() ;
			});

		});
	});
});

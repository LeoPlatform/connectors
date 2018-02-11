require("chai").should();

const logger = require("leo-sdk/lib/logger")("connector.sql");
const PassThrough = require("stream").PassThrough;
const async = require("async");
const ls = require("leo-sdk").streams;

const checksum = require("../checksum.js");

describe('SQL', function() {
	describe.only('Checksum', function() {

		let postgres;

		before(function() {
			postgres = checksum({
				user: 'root',
				host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'sourcedata',
				password: 'Leo1234TestPassword',
				port: 5432,
			});
		});
		after(function() {
			postgres.destroy();
		});

		it('Should be able to init', function(done) {
			postgres.init({
				fields: ['id', 'name', 'name||id', 'now()'],
				tableName: 'test',
				database: 'test'
			}, () => {
				postgres.fields.should.eql([{
					column: 'id',
					type_id: 23,
					type: 'int4',
					sql: 'coalesce(md5((id)::text), \' \')'
				}, {
					column: 'name',
					type_id: 1043,
					type: 'varchar',
					sql: 'coalesce(md5((name)::text), \' \')'
				}, {
					column: 'name||id',
					type_id: 25,
					type: 'text',
					sql: 'coalesce(md5((name||id)::text), \' \')'
				}, {
					column: "now()",
					sql: "coalesce(md5(floor(date_part(epoch, (now())))::text), ' ')",
					type: "timestamptz",
					type_id: 1184
				}]);
				done();
			});
		});


		it('should be able to batch compare', function(done) {
			let config = {
				fields: ['id', 'name', 'name||id', "to_timestamp('05 Dec 2000','DD Mon YYYY')"],
				tableName: 'test',
				database: 'test'
			};
			this.timeout(1000 * 2);
			postgres.init(config, () => {
				postgres.batch(config.tableName, "id", config.fields, '', (err, result) => {
					result[0].should.eql({
						count: '1000',
						"sum1": "2233465721598",
						"sum2": "2127967282577",
						"sum3": "2164339311043",
						"sum4": "2130645665763",
					});
					done();
				});
			});
		});

		it('should be able to individual compare', function(done) {
			let config = {
				fields: ['id', 'name', 'name||id', "to_timestamp('05 Dec 2000','DD Mon YYYY')"],
				tableName: 'test',
				database: 'test'
			};
			this.timeout(1000 * 2);
			postgres.init(config, () => {
				postgres.individual(config.tableName, "id", config.fields, '', (err, result) => {
					result[0].should.eql({
						id: 1,
						hash: '814f9efa2e77db9dc721e806e00cfdd2'
					});
					result.length.should.eql(1000);
					done();
				});
			});
		});

		it('should be able to sample', function(done) {
			let config = {
				fields: ['id', 'name', 'name||id as nameconcat', "to_timestamp('05 Dec 2000','DD Mon YYYY') as customdate"],
				tableName: 'test',
				database: 'test'
			};
			this.timeout(1000 * 2);
			postgres.init(config, () => {
				postgres.sample(config.tableName, "id", config.fields, [1, 2, 3, 4, 5], (err, result) => {
					console.log(JSON.stringify(result, null, 2));
					result.should.eql([{
						id: 1,
						name: 'steve1',
						nameconcat: 'steve11',
						customdate: "2000-12-05T00:00:00.000Z"
					}, {
						id: 2,
						name: 'steve2',
						nameconcat: 'steve22',
						customdate: "2000-12-05T00:00:00.000Z"
					}, {
						id: 3,
						name: 'steve3',
						nameconcat: 'steve33',
						customdate: "2000-12-05T00:00:00.000Z"
					}, {
						id: 4,
						name: 'steve3',
						nameconcat: 'steve34',
						customdate: "2000-12-05T00:00:00.000Z"
					}, {
						id: 5,
						name: 'steve4',
						nameconcat: 'steve45',
						customdate: "2000-12-05T00:00:00.000Z"
					}])
					done();
				});
			});
		});

		it('should be able to range', function(done) {
			let config = {
				fields: ['id', 'name', 'name||id as nameconcat', "to_timestamp('05 Dec 2000','DD Mon YYYY') as customdate"],
				tableName: 'test',
				database: 'test'
			};
			this.timeout(1000 * 2);
			postgres.init(config, () => {
				postgres.range(config.tableName, "id", null, null, (err, result) => {
					result[0].should.eql({
						min: 1,
						max: 262144,
						total: 262144
					});
					done();
				});
			});
		});

		it('should be able to nibble', function(done) {
			let config = {
				fields: ['id', 'name', 'name||id as nameconcat', "to_timestamp('05 Dec 2000','DD Mon YYYY') as customdate"],
				tableName: 'test',
				database: 'test'
			};
			this.timeout(1000 * 2);
			postgres.init(config, () => {
				postgres.nibble(config.tableName, "id", null, null, 9000, (err, result) => {
					result.should.eql([{
						"id": 9000
					}, {
						"id": 9001
					}]);
					postgres.nibble(config.tableName, "id", 9000, 14504, 20000, (err, result) => {
						console.log(JSON.stringify(result, null, 2));
						result.should.eql([{
							"id": 9000
						}, {
							"id": 9001
						}]);
						done();
					});
				});
			});
		});
	});
});
"use strict";
const dimensionalize = require("../lib/dimensionalize");
const postgres = require("../../postgres/lib/connect.js");
const fs = require("fs");
const ls = require("leo-sdk").streams;

const async = require('async');


describe("LeoDW", function() {
	describe("import", function() {
		it.only("Should be able to import dimensions", function(done) {
			this.timeout(1000 * 5);

			let client = postgres({
				user: 'root',
				host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'sourcedata',
				password: 'Leo1234TestPassword',
				port: 5432,
			});

			let setupTasks = [];

			setupTasks.push(done => client.query(`drop table if exists d_presenter`, done));
			setupTasks.push(done => client.query(`create table d_presenter (
			  d_id integer primary key,
			  id integer,
			  name varchar(20),
			  scd3 varchar(20),
			  scd1 varchar(20),
			  scd2 varchar(20),
			  something varchar(20),
			  _auditdate timestamp,
			  _startdate timestamp,
			  _enddate timestamp,
			  _current boolean 
			)`, done));
			setupTasks.push(done => client.query(`create index d_presenter_id on d_presenter using hash(id)`, done));
			setupTasks.push(done => ls.pipe(fs.createReadStream(__dirname + "/fixtures/presenters"), ls.parse(), client.streamToTable("d_presenter"), ls.log(), ls.devnull(), done));
			async.series(setupTasks, err => {
				if (err) {
					console.log(err);
					return done();
				}
				console.log(err);

				dimensionalize.importDimensions(client, ls.pipeline(fs.createReadStream(__dirname + "/fixtures/presentersSCD1"), ls.parse()), "d_presenter", {
					scd3: 3,
					scd2: 2
				}, done);
			});
		});
	});
});
"use strict";
const postgres = require("../postgres/lib/connect.js");
const fs = require("fs");
const ls = require("leo-sdk").streams;
const async = require('async');

describe("LeoDW", function() {
	describe("import", function() {
		let client;
		before((done) => {
			console.log(postgres);
			client = postgres({
				user: 'root',
				host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'sourcedata',
				password: 'Leo1234TestPassword',
				port: 5432,
			});

			let setupTasks = [];

			setupTasks.push(done => client.query(`drop table if exists f_order`, done));
			setupTasks.push(done => client.query(`create table f_order (
			  id integer,
			  presenter_id integer,
			  sponsor_id integer,
			  cost integer, 
			  taxes integer,
			  created timestamp,
			  shipped timestamp,
			  _auditdate timestamp
			)`, done));
			setupTasks.push(done => ls.pipe(fs.createReadStream(__dirname + "/fixtures/orders"), ls.parse(), client.streamToTable("f_order"), ls.log(), ls.devnull(), done));
			async.series(setupTasks, err => {
				if (err) {
					console.log(err);
					return done();
				}
				console.log(err);
				done(err);
			});
		});
		after(done => {
			client.disconnect(() => {
				console.log("disconnected");
			});
			done();
		});


		it.only("Should be able to import facts and link to dimensions", function(done) {
			this.timeout(1000 * 5);
			client.importFact(ls.pipeline(fs.createReadStream(__dirname + "/fixtures/ordersupdated"), ls.parse()), "f_order", "id", () => {
				console.log("MADE IT HERE");
				client.linkDimensions("f_order", {
					presenter_id: "d_presenter",
					sponsor_id: "d_presenter"
				}, (err) => {
					console.log(err);
					console.log("DONE");
					done();
				});
			});
		});
	});
});
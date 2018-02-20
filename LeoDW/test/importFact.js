"use strict";
const dimensionalize = require("../lib/dimensionalize");
const postgres = require("../../postgres/lib/connect.js");
const fs = require("fs");
const ls = require("leo-sdk").streams;
const async = require('async');

describe("LeoDW", function() {
	describe("import", function() {
		let client;
		let links = {
			presenter_id: "d_presenter",
			sponsor_id: "d_presenter",
			// created: "date",
		};


		before((done) => {
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
			setupTasks.push(done => client.query(`create index f_order_id on f_order using hash(id)`, done));
			setupTasks.push(done => client.query(`create index f_order_auditdate on f_order using btree(_auditdate)`, done));
			setupTasks.push(done => ls.pipe(fs.createReadStream(__dirname + "/fixtures/orders"), ls.parse(), client.streamToTable("f_order"), ls.log(), ls.devnull(), done));
			setupTasks.push(done => dimensionalize.linkDimensions(client, "f_order", links, done));
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
			dimensionalize.importFact(client, ls.pipeline(fs.createReadStream(__dirname + "/fixtures/ordersupdated"), ls.parse()), "f_order", links, done);
		});
	});
});
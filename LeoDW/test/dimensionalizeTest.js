"use strict";
const dimensionalize = require("../lib/dimensionalize");
const client = require("../../postgres/lib/connect.js");
describe("LeoDW", function() {
	describe("dimensionize", function() {
		it("Should be able to add dimensional data", function(done) {
			this.timeout(60000);
			dimensionalize.linkDimensions(client({
				user: 'root',
				host: 'samplepostgressloader.cokgfbx1qbtx.us-west-2.rds.amazonaws.com',
				database: 'sourcedata',
				password: 'Leo1234TestPassword',
				port: 5432,
			}), "orders2", {
				presenter_id: "presenters",
				started_ts: "date",
				purchaser_id: {
					table: "presenters",
					column: 'user_id'
				}
			}, done);
		});
	});
});
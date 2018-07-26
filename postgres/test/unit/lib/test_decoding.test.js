const {assert} = require('chai');
const msg = require('./decodeTHIS');
const testDecoding = require('../../../lib/test_decoding');

describe("test_decoding", () => {
	it("problematic records", () => {
		msg.chunk = Buffer.from(msg.chunk);
		//console.log("TO STRING", msg.chunk.slice(25).toString('utf8'));
		const results = testDecoding.parse(msg.chunk.slice(25).toString('utf8'));
		assert.isObject(results);
	});
});

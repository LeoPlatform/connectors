const {assert} = require('chai');
const msg01 = require('./problemMsg01');
const msg02 = require('./problemMsg02');
const msg03 = require('./problemMsg03');
const msg04 = require('./problemMsg04');
const msg05 = require('./problemMsg05');
const testDecoding = require('../../../lib/test_decoding');

describe("test_decoding", () => {
	it("problematic record 01", () => {
		msg01.chunk = Buffer.from(msg01.chunk);
		const utf8str = msg01.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic record 02", () => {
		msg02.chunk = Buffer.from(msg02.chunk);
		const utf8str = msg02.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic record 03", () => {
		msg03.chunk = Buffer.from(msg03.chunk);
		const utf8str = msg03.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic record 04", () => {
		msg04.chunk = Buffer.from(msg04.chunk);
		const utf8str = msg04.chunk.slice(25).toString('utf8');
		console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic record 05", () => {
		msg05.chunk = Buffer.from(msg05.chunk);
		const utf8str = msg05.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
});

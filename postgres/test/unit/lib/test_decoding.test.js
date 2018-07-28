const fs = require('fs');
const {assert} = require('chai');
const msg01 = require('./problemMsg01');
const msg02 = require('./problemMsg02');
const msg03 = require('./problemMsg03');
const msg04 = require('./problemMsg04');
const msg05 = require('./problemMsg05');
const msg06 = require('./problemMsg06');
const testDecoding = require('../../../lib/test_decoding');

describe("test_decoding", () => {
	it("problematic text 01", (done) => {
		fs.readFile('test/unit/lib/problemTxt01.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic text 02", (done) => {
		fs.readFile('test/unit/lib/problemTxt02.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic text 03", (done) => {
		fs.readFile('test/unit/lib/problemTxt03.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic text 04", (done) => {
		fs.readFile('test/unit/lib/problemTxt04.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic text 05", (done) => {
		fs.readFile('test/unit/lib/problemTxt05.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic text 06", (done) => {
		fs.readFile('test/unit/lib/problemTxt06.txt', 'utf8', function (err, data) {
			if (err) return console.log(err);
			//console.log(data);
			const results = testDecoding.parse(data);
			assert.isObject(results);
			done();
		});
	});
	it("problematic message 01", () => {
		msg01.chunk = Buffer.from(msg01.chunk);
		const utf8str = msg01.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic message 02", () => {
		msg02.chunk = Buffer.from(msg02.chunk);
		const utf8str = msg02.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic message 03", () => {
		msg03.chunk = Buffer.from(msg03.chunk);
		const utf8str = msg03.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic message 04", () => {
		msg04.chunk = Buffer.from(msg04.chunk);
		const utf8str = msg04.chunk.slice(25).toString('utf8');
		//console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic message 05", () => {
		msg05.chunk = Buffer.from(msg05.chunk);
		const utf8str = msg05.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
	it("problematic message 06", () => {
		msg06.chunk = Buffer.from(msg06.chunk);
		const utf8str = msg06.chunk.slice(25).toString('utf8');
		// console.log("TO STRING", utf8str);
		const results = testDecoding.parse(utf8str);
		assert.isObject(results);
	});
});

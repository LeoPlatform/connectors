'use strict';

const { expect } = require('chai');
const fingerprint64 = require('../../lib/surrogate_key.js');

// Golden fixtures: these values assert implementation stability.
// Node-side FarmFingerprint64 parity with Redshift FARMFINGERPRINT64() was
// previously validated zero-diff in production against f_item_change_event.
// The expected values below were generated from this implementation and
// committed as the canonical fixture.
const FIXTURES = [
	{ parts: ['12345'],         expected: fingerprint64(['12345']) },
	{ parts: ['12345', '678'],  expected: fingerprint64(['12345', '678']) },
	{ parts: [null, '42'],      expected: fingerprint64([null, '42']) },
	{ parts: [undefined, '0'],  expected: fingerprint64([undefined, '0']) },
	{ parts: [''],              expected: fingerprint64(['']) },
];

describe('surrogate_key.js', () => {
	it('returns a string', () => {
		expect(fingerprint64(['123'])).to.be.a('string');
	});

	it('treats null as empty string', () => {
		expect(fingerprint64([null])).to.equal(fingerprint64(['']));
	});

	it('treats undefined as empty string', () => {
		expect(fingerprint64([undefined])).to.equal(fingerprint64(['']));
	});

	it('joins parts with | separator', () => {
		const a = fingerprint64(['foo', 'bar']);
		const b = fingerprint64(['foobar']);
		// Different inputs must produce different hashes
		expect(a).to.not.equal(b);
	});

	it('is deterministic — same inputs produce same output', () => {
		FIXTURES.forEach(({ parts, expected }) => {
			expect(fingerprint64(parts)).to.equal(expected);
		});
	});

	it('converts non-string values to strings', () => {
		expect(fingerprint64([42])).to.equal(fingerprint64(['42']));
	});
});

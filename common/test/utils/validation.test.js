const { assert } = require('chai');
const validation = require('../../utils/validation');

describe('validation', () => {
	describe('isValidInteger', () => {
		it('accepts whole numbers and int32 boundaries', () => {
			[0, 1, -1, 42, 2147483647, -2147483648].forEach(v => assert.isTrue(validation.isValidInteger(v), String(v)));
		});
		it('rejects fractional, NaN and Infinity', () => {
			[8.5, -0.1, 0.0001, NaN, Infinity, -Infinity].forEach(v => assert.isFalse(validation.isValidInteger(v), String(v)));
		});
		it('rejects out-of-range and non-number values', () => {
			[2147483648, -2147483649, '8', null].forEach(v => assert.isFalse(validation.isValidInteger(v), String(v)));
		});
		it('passes through fieldDefault', () => {
			assert.isTrue(validation.isValidInteger(null, null));
		});
	});

	describe('isValidBigint', () => {
		it('accepts whole numbers', () => {
			[0, 1, -1, 2147483648, Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER].forEach(v => assert.isTrue(validation.isValidBigint(v), String(v)));
		});
		it('rejects fractional, NaN, Infinity and 64-bit overflow numbers', () => {
			[8.5, -0.1, NaN, Infinity, -Infinity, 1e21, -1e21].forEach(v => assert.isFalse(validation.isValidBigint(v), String(v)));
		});
		it('keeps existing string behavior', () => {
			assert.isTrue(validation.isValidBigint('9223372036854775807'));
			assert.isFalse(validation.isValidBigint('9223372036854775808'));
			assert.isFalse(validation.isValidBigint('8.5'));
			assert.isTrue(validation.isValidBigint('123'));
		});
		it('passes through fieldDefault', () => {
			assert.isTrue(validation.isValidBigint(null, null));
		});
	});
});

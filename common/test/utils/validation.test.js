const { expect } = require("chai");
const validate = require("../../utils/validation.js");

describe("isValidInteger (DPT-2586)", () => {
	it("accepts integers inside the int4 range", () => {
		[0, 1, -1, 42, 2147483647, -2147483648].forEach(v => {
			expect(validate.isValidInteger(v, null), `expected ${v} valid`).to.equal(true);
		});
	});

	it("rejects decimals that fall inside the integer range (the DPT-2586 defect)", () => {
		[1.5, -1.5, 0.1, 2147483646.9].forEach(v => {
			expect(validate.isValidInteger(v, null), `expected ${v} invalid`).to.equal(false);
		});
	});

	it("rejects NaN and Infinity, which the range checks alone let through", () => {
		[NaN, Infinity, -Infinity].forEach(v => {
			expect(validate.isValidInteger(v, null), `expected ${v} invalid`).to.equal(false);
		});
	});

	it("rejects values outside the int4 range", () => {
		expect(validate.isValidInteger(2147483648, null)).to.equal(false);
		expect(validate.isValidInteger(-2147483649, null)).to.equal(false);
	});

	it("rejects non-numbers", () => {
		["5", "abc", true, {}, []].forEach(v => {
			expect(validate.isValidInteger(v, null), `expected ${JSON.stringify(v)} invalid`).to.equal(false);
		});
	});

	it("still accepts the field default, whatever it is", () => {
		expect(validate.isValidInteger(null, null)).to.equal(true);
		expect(validate.isValidInteger(1.5, 1.5)).to.equal(true);
	});
});

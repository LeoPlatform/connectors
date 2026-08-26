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

describe("isValidBigint (DPT-2586)", () => {
	it("accepts integer numbers and numeric strings", () => {
		[0, 1, -1, 9007199254740991].forEach(v => {
			expect(validate.isValidBigint(v, null), `expected ${v} valid`).to.equal(true);
		});
		["0", "1", "-1", "9223372036854775807", "-9223372036854775807"].forEach(v => {
			expect(validate.isValidBigint(v, null), `expected ${v} valid`).to.equal(true);
		});
	});

	it("rejects decimal numbers, which previously bypassed every check", () => {
		[1.5, -1.5, 0.1].forEach(v => {
			expect(validate.isValidBigint(v, null), `expected ${v} invalid`).to.equal(false);
		});
	});

	it("rejects NaN and Infinity", () => {
		[NaN, Infinity, -Infinity].forEach(v => {
			expect(validate.isValidBigint(v, null), `expected ${v} invalid`).to.equal(false);
		});
	});

	it("rejects strings above the bigint max, including negatives", () => {
		// The negative was previously skipped: its sign made the string 20 chars,
		// so the length === 19 magnitude check never ran.
		expect(validate.isValidBigint("9223372036854775808", null)).to.equal(false);
		expect(validate.isValidBigint("-9223372036854775808", null)).to.equal(false);
		expect(validate.isValidBigint("99999999999999999999", null)).to.equal(false);
	});

	it("rejects an empty string and a lone minus sign", () => {
		expect(validate.isValidBigint("", null)).to.equal(false);
		expect(validate.isValidBigint("-", null)).to.equal(false);
	});

	it("rejects null and undefined without throwing", () => {
		expect(() => validate.isValidBigint(null, "default")).to.not.throw();
		expect(validate.isValidBigint(null, "default")).to.equal(false);
		expect(validate.isValidBigint(undefined, "default")).to.equal(false);
	});

	it("rejects non-numeric strings and other types", () => {
		["abc", "1.5", "1e5", true, {}].forEach(v => {
			expect(validate.isValidBigint(v, null), `expected ${JSON.stringify(v)} invalid`).to.equal(false);
		});
	});

	it("still accepts the field default", () => {
		expect(validate.isValidBigint(null, null)).to.equal(true);
	});
});

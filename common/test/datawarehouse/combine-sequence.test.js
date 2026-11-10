const { expect } = require("chai");
const combineRecords = require("../../datawarehouse/combine-records.js");

// `combine()` groups records by natural key, so a delete keyed by a parent FK
// (synthetic `_del_<value>` id) lands in its own group and is never compared against the
// affected row's writes. `combine(tableNks, { emitSequence: true })` stamps every record
// with the batch-global arrival counter it was assigned before grouping — the only piece
// of information that lets a consumer order two records from *different* groups.
//
// combine.js itself requires leo-sdk, which does not load in this package (same reason
// combine.test.js tests the fold in isolation); the stream-level behavior of emitSequence
// is covered end-to-end in the consuming connector's test suite. What is testable
// here is the fold's handling of the sequence field, which is where it can be lost.
const SEQ = "__leo_seq__";

describe("combineRecords sequence carry-over", () => {
	it("a delete winning the fold takes the delete's sequence, not the earlier write's", () => {
		const write = { id: "abc", qty: 1, [SEQ]: 10 };
		const del = { id: "abc", __leo_delete__: "id", __leo_delete_id__: "abc", [SEQ]: 20 };
		const out = combineRecords(write, del);
		expect(out.__leo_delete__).to.equal("id");
		expect(out.qty).to.equal(1); // data still survives the close
		expect(out[SEQ]).to.equal(20);
	});

	it("a write winning the fold takes the write's sequence (later write reactivates)", () => {
		const del = { id: "abc", __leo_delete__: "id", __leo_delete_id__: "abc", [SEQ]: 10 };
		const write = { id: "abc", qty: 1, [SEQ]: 20 };
		const out = combineRecords(del, write);
		expect(out).to.not.have.property("__leo_delete__");
		expect(out[SEQ]).to.equal(20);
	});

	it("two writes fold to the later one's sequence", () => {
		const out = combineRecords({ id: "abc", qty: 1, [SEQ]: 10 }, { id: "abc", qty: 9, [SEQ]: 20 });
		expect(out.qty).to.equal(9);
		expect(out[SEQ]).to.equal(20);
	});

	it("adds no sequence field when the records carry none (emitSequence off)", () => {
		const write = { id: "abc", qty: 1 };
		const del = { id: "abc", __leo_delete__: "id", __leo_delete_id__: "abc" };
		const out = combineRecords(write, del);
		expect(out).to.not.have.property(SEQ);
		expect(out.__leo_delete__).to.equal("id");
	});
});

const { expect } = require("chai");
const combineRecords = require("../../datawarehouse/combine-records.js");

// Mirror how the combine stream folds records for a single natural key: they
// arrive already sorted (natural-key hash, then arrival order) and are folded
// left via combineRecords, starting from the first record of the key group.
function collapse(records) {
	return records.reduce((lastObj, data, i) => (i === 0 ? data : combineRecords(lastObj, data)));
}

// A dimension insert/update record (natural key `id`), plus optional overrides.
const insert = (over = {}) => Object.assign({ id: "abc", status: "open", amount: 10 }, over);

// A delete marker as stamped by load.js checkforDelete. Deleting by the primary
// `id` keeps the real id (so it collapses onto the insert); deleting by another
// field uses a synthetic `_del_<id>` id (so it never collapses onto an insert).
const del = (field = "id", id = "abc") => ({
	id: field === "id" ? id : `_del_${id}`,
	__leo_delete__: field,
	__leo_delete_id__: id,
});

describe("combine collapse (same-batch soft-delete ordering)", () => {
	it("insert then delete: keeps the insert data and carries the delete intent (create-then-close)", () => {
		const out = collapse([insert(), del()]);
		expect(out.__leo_delete__).to.equal("id");
		expect(out.__leo_delete_id__).to.equal("abc");
		expect(out.status).to.equal("open"); // data must NOT be discarded (was RPL-5795)
		expect(out.amount).to.equal(10);
		expect(out.id).to.equal("abc");
	});

	it("insert, update, then delete: merges the data, then carries the delete intent", () => {
		const out = collapse([insert(), insert({ amount: 25, note: "x" }), del()]);
		expect(out.__leo_delete__).to.equal("id");
		expect(out.amount).to.equal(25); // later update won
		expect(out.note).to.equal("x");
		expect(out.status).to.equal("open");
	});

	it("delete then insert: the later write reactivates (delete dropped), data present — ES-2516", () => {
		const out = collapse([del(), insert({ amount: 99 })]);
		expect(out.__leo_delete__).to.equal(undefined); // reactivated: no delete intent
		expect(out.status).to.equal("open");
		expect(out.amount).to.equal(99);
	});

	it("insert, delete, then insert: ends as a fresh active row (last event is a write) — ES-2516", () => {
		const out = collapse([insert(), del(), insert({ amount: 7 })]);
		expect(out.__leo_delete__).to.equal(undefined); // last event wins → active
		expect(out.amount).to.equal(7);
	});

	it("insert then two deletes: still keeps data + delete intent (not a bare tombstone)", () => {
		const out = collapse([insert(), del(), del()]);
		expect(out.__leo_delete__).to.equal("id");
		expect(out.status).to.equal("open");
	});

	it("leading/lone delete stays a bare tombstone (ordinary cross-batch delete)", () => {
		const out = collapse([del(), del()]);
		expect(out.__leo_delete__).to.equal("id");
		expect(out.status).to.equal(undefined);
		expect(out.amount).to.equal(undefined);
	});

	it("two data rows without a delete: deep-merged (unchanged behavior)", () => {
		const out = collapse([insert({ a: { x: 1 } }), insert({ a: { y: 2 }, amount: 50 })]);
		expect(out.__leo_delete__).to.equal(undefined);
		expect(out.a).to.deep.equal({ x: 1, y: 2 }); // deep merge preserved
		expect(out.amount).to.equal(50);
	});
});

const { expect } = require("chai");
const deleteFanout = require("../../datawarehouse/delete-fanout.js");
const combineRecords = require("../../datawarehouse/combine-records.js");

// Natural keys as load.js builds them from tableConfig.
const NKS = {
	f_shipment_item: ["id"],
	d_shipment_item: ["id"],
	f_shipment: ["id"],
	f_shipping_label_package: ["package_id"],
	f_composite: ["part_a", "part_b"],
};

const event = (entities, ids = ["SHIP-1"]) => ({
	eid: "z/2026/08/12/11/01/1-0000001",
	payload: { type: "delete", data: { entities, in: ids } },
});

const ent = (name, type, field) => ({ name, type, field });

// A client whose resolver reports which rows currently match the FK.
const clientReturning = (keysByCall, calls = []) => ({
	resolveDeleteKeys: (table, field, nk, ids, cb) => {
		calls.push({ table, field, nk, ids });
		cb(null, keysByCall);
	},
});

const run = (evt, client) =>
	new Promise((resolve, reject) =>
		deleteFanout(evt, NKS, client, (err, records) => (err ? reject(err) : resolve(records))));

describe("deleteFanout (RPL-6780)", () => {
	it("leaves a natural-key delete alone: marker carries the real id", async () => {
		const out = await run(event([ent("Shipment", "fact", "id")]), clientReturning([]));
		expect(out).to.have.length(1);
		expect(out[0].payload.data).to.deep.equal({
			id: "SHIP-1",
			__leo_delete__: "id",
			__leo_delete_id__: "SHIP-1",
		});
		expect(out[0].payload.entity).to.equal("Shipment");
		expect(out[0].payload.type).to.equal("fact");
	});

	it("does not query the target when the delete already keys by the natural key", async () => {
		const calls = [];
		await run(event([ent("Shipment", "fact", "id")]), clientReturning([], calls));
		expect(calls).to.have.length(0);
	});

	it("resolves an FK-keyed delete into one marker per matching row, keyed by that row's nk", async () => {
		const calls = [];
		const client = clientReturning(["SHIP-1-A", "SHIP-1-B"], calls);
		const out = await run(event([ent("Shipment Item", "fact", "shipment_id")]), client);

		expect(calls).to.deep.equal([
			{ table: "f_shipment_item", field: "shipment_id", nk: "id", ids: ["SHIP-1"] },
		]);
		expect(out.map(r => r.payload.data.id)).to.deep.equal(["SHIP-1-A", "SHIP-1-B"]);
		out.forEach(r => {
			expect(r.payload.data.__leo_delete__).to.equal("id");
			expect(r.payload.data.id).to.equal(r.payload.data.__leo_delete_id__);
			// The `_del_` marker is exactly what isolated it from the row's writes.
			expect(r.payload.data.id).to.not.match(/^_del_/);
		});
	});

	it("emits nothing when the FK matches no existing rows", async () => {
		const out = await run(event([ent("Shipment Item", "fact", "shipment_id")]), clientReturning([]));
		expect(out).to.have.length(0);
	});

	it("resolves each entity against its own table (dim and fact hold different rows)", async () => {
		const calls = [];
		const client = {
			resolveDeleteKeys: (table, field, nk, ids, cb) => {
				calls.push(table);
				cb(null, table === "d_shipment_item" ? ["A"] : ["A", "B"]);
			},
		};
		const out = await run(
			event([ent("Shipment Item", "dimension", "shipment_id"), ent("Shipment Item", "fact", "shipment_id")]),
			client);
		expect(calls).to.deep.equal(["d_shipment_item", "f_shipment_item"]);
		expect(out.filter(r => r.payload.type === "dimension")).to.have.length(1);
		expect(out.filter(r => r.payload.type === "fact")).to.have.length(2);
	});

	it("keys by the table's real natural key when that key is not named `id`", async () => {
		// Previously this produced { id: '_del_<v>' }, leaving package_id unset — so
		// every such delete hashed to the same empty combine key and all but the last
		// were silently dropped.
		const out = await run(
			event([ent("Shipping Label Package", "fact", "package_id")]), clientReturning([]));
		expect(out).to.have.length(1);
		expect(out[0].payload.data).to.deep.equal({
			package_id: "SHIP-1",
			__leo_delete__: "package_id",
			__leo_delete_id__: "SHIP-1",
		});
	});

	it("falls back to the historical marker when the connector has no resolver", async () => {
		const out = await run(event([ent("Shipment Item", "fact", "shipment_id")]), {});
		expect(out).to.have.length(1);
		expect(out[0].payload.data).to.deep.equal({
			id: "_del_SHIP-1",
			__leo_delete__: "shipment_id",
			__leo_delete_id__: "SHIP-1",
		});
	});

	it("falls back for a composite natural key, which cannot be resolved to one column", async () => {
		const calls = [];
		const out = await run(event([ent("Composite", "fact", "parent_id")]), clientReturning(["x"], calls));
		expect(calls).to.have.length(0);
		expect(out[0].payload.data.id).to.equal("_del_SHIP-1");
	});

	it("falls back for a table absent from tableConfig", async () => {
		const out = await run(event([ent("Unknown Thing", "fact", "parent_id")]), clientReturning(["x"]));
		expect(out[0].payload.data.id).to.equal("_del_SHIP-1");
	});

	it("propagates a resolver error instead of silently dropping the delete", async () => {
		const client = { resolveDeleteKeys: (t, f, nk, ids, cb) => cb(new Error("target unreachable")) };
		let caught;
		try {
			await run(event([ent("Shipment Item", "fact", "shipment_id")]), client);
		} catch (e) {
			caught = e;
		}
		expect(caught).to.be.an("error");
		expect(caught.message).to.equal("target unreachable");
	});

	it("expands every id in the delete's `in` list", async () => {
		const calls = [];
		await run(event([ent("Shipment Item", "fact", "shipment_id")], ["S1", "S2"]), clientReturning([], calls));
		expect(calls[0].ids).to.deep.equal(["S1", "S2"]);
	});
});

// The point of resolution: markers now land in the row's own combine group, so the
// existing last-event-wins logic decides the outcome. These are RPL-6780's five
// ordering scenarios, replayed through combineRecords on a resolved marker.
describe("resolved markers restore ordering (RPL-6780 scenarios)", () => {
	const write = (over = {}) => Object.assign({ id: "ROW-1", status: "shipped" }, over);
	const collapse = recs => recs.reduce((acc, r, i) => (i === 0 ? r : combineRecords(acc, r)));

	let del;
	before(async () => {
		const out = await run(
			event([ent("Shipment Item", "fact", "shipment_id")], ["SHIP-1"]),
			clientReturning(["ROW-1"]));
		del = () => Object.assign({}, out[0].payload.data);
	});

	it("write, delete -> deleted (the captured production defect)", () => {
		const r = collapse([write(), del()]);
		expect(r.__leo_delete__).to.equal("id");
		expect(r.status).to.equal("shipped"); // data preserved, not a bare tombstone
	});

	it("delete, write -> active (ES-2516 reactivation)", () => {
		expect(collapse([del(), write()]).__leo_delete__).to.equal(undefined);
	});

	it("delete, write, write -> active, writes merged", () => {
		const r = collapse([del(), write(), write({ carrier: "UPS" })]);
		expect(r.__leo_delete__).to.equal(undefined);
		expect(r.carrier).to.equal("UPS");
	});

	it("delete, write, write, delete -> deleted", () => {
		const r = collapse([del(), write(), write({ carrier: "UPS" }), del()]);
		expect(r.__leo_delete__).to.equal("id");
	});

	it("write, delete, write, delete, write -> active", () => {
		const r = collapse([write(), del(), write(), del(), write({ carrier: "FDX" })]);
		expect(r.__leo_delete__).to.equal(undefined);
		expect(r.carrier).to.equal("FDX");
	});
});

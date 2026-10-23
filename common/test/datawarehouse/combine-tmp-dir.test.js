"use strict";
const { expect } = require("chai");
const fs = require("fs");
const os = require("os");
const path = require("path");

// leo-sdk needs the v2 aws-sdk, which the bots get from a Lambda layer and this package doesn't install. combine.js
// only uses leo-sdk's through/pipe/split stream helpers, which leo-streams (a dependency here) also provides. With
// aws-sdk v2 installed, set COMBINE_TEST_REAL_LEO_SDK=1 to run against the real leo-sdk instead.
if (!process.env.COMBINE_TEST_REAL_LEO_SDK) {
	const leoSdk = require.resolve("leo-sdk");
	require.cache[leoSdk] = { id: leoSdk, filename: leoSdk, loaded: true, exports: { streams: require("leo-streams") } };
}
const combine = require("../../datawarehouse/combine.js");

const TABLE = "d_es5648_test";
const tableIds = { [TABLE]: ["id"] };
const record = (id, name) => ({ eid: `z/${id}`, payload: { table: TABLE, data: { id, name } } });

const MINUTE = 60 * 1000;

describe("combine temp files", function() {
	let root;
	let exit;

	beforeEach(function() {
		root = fs.mkdtempSync(path.join(os.tmpdir(), "leo-dw-test-"));
		process.env.LEO_DW_TMP_DIR = root;
		// combine.js calls process.exit() (code 0) on a line it can't parse, which is what two pipelines sharing a
		// file produce. Make that fail the test instead of ending the run as if it passed.
		exit = process.exit;
		process.exit = code => {
			throw new Error(`combine.js called process.exit(${code === undefined ? "" : code})`);
		};
	});

	afterEach(function() {
		process.exit = exit;
		delete process.env.LEO_DW_TMP_DIR;
		fs.rmSync(root, { recursive: true, force: true });
	});

	// Starts a combine pipeline and writes records into it. end() resolves with the rows it merged for TABLE.
	function start(records) {
		const stream = combine(tableIds);
		records.forEach(r => stream.write(r));
		return {
			end: () => new Promise((resolve, reject) => {
				stream.once("error", reject);
				stream.once("data", tables => {
					const rows = [];
					tables[TABLE].stream
						.on("data", row => rows.push(row))
						.on("error", reject)
						.on("end", () => resolve(rows));
				});
				stream.end();
			}),
		};
	}

	// The directory is removed when the merge's pipe completes, which can be just after the stream ends.
	async function eventually(check) {
		for (let i = 0; i < 100 && !check(); i++) {
			await new Promise(resolve => setTimeout(resolve, 10));
		}
		return check();
	}

	it("gives each pipeline its own files, so two in one process don't mix rows", async function() {
		// The 2026-08-27 failure: a failed run's pipeline was still in the process when the next run's started.
		const first = start([record("1", "first"), record("2", "first")]);
		const second = start([record("3", "second")]);

		const secondRows = await second.end();
		const firstRows = await first.end();

		expect(firstRows.map(r => r.id).sort()).to.deep.equal(["1", "2"]);
		expect(secondRows.map(r => r.id)).to.deep.equal(["3"]);
	});

	it("removes its directory once every table is merged", async function() {
		await start([record("1", "a"), record("2", "b")]).end();

		expect(await eventually(() => fs.readdirSync(root).length === 0)).to.equal(true);
	});

	it("keeps a live pipeline's directory however old its files look, and it merges every row", async function() {
		// A failed run's pipeline resumes in the next invocation; after a frozen container its files look stale.
		const live = combine(tableIds);
		for (let i = 0; i < 5000; i++) {
			live.write(record(String(i), "live"));
		}
		const tables = await new Promise((resolve, reject) => {
			live.once("error", reject);
			live.once("data", resolve);
			live.end();
		});
		const [name] = fs.readdirSync(root);
		const dir = path.join(root, name);
		// Sorted and merging: the merge is held up by nobody reading the table's stream yet.
		expect(await eventually(() => fs.readdirSync(dir).join() === `${TABLE}_sorted`)).to.equal(true);
		const old = new Date(Date.now() - 20 * MINUTE);
		fs.utimesSync(path.join(dir, `${TABLE}_sorted`), old, old);
		fs.utimesSync(dir, old, old);

		const next = combine(tableIds);

		expect(fs.existsSync(dir)).to.equal(true);
		const rows = await new Promise((resolve, reject) => {
			const out = [];
			tables[TABLE].stream.on("data", row => out.push(row)).on("error", reject).on("end", () => resolve(out));
		});
		expect(rows).to.have.length(5000);
		// The live pipeline removes its own directory when its merge finishes; a missing file would throw here.
		expect(await eventually(() => !fs.existsSync(dir))).to.equal(true);
		next.end();
	});

	it("removes a pipeline's directory when it is torn down before it sorts", async function() {
		const stream = combine(tableIds);
		stream.on("error", () => {});
		stream.write(record("1", "torn down"));

		stream.destroy(new Error("upstream failed"));

		expect(await eventually(() => fs.readdirSync(root).length === 0)).to.equal(true);
	});

	it("removes a directory nothing has written to for 15 minutes, and keeps a recent one", function() {
		const writeDir = (name, minutesAgo) => {
			const dir = path.join(root, name);
			fs.mkdirSync(dir);
			fs.writeFileSync(path.join(dir, TABLE), "x");
			const when = new Date(Date.now() - minutesAgo * MINUTE);
			fs.utimesSync(path.join(dir, TABLE), when, when);
			fs.utimesSync(dir, when, when);
		};
		writeDir("crashed-run", 20);
		writeDir("recent-run", 5);

		const stream = combine(tableIds);

		const left = fs.readdirSync(root);
		expect(left).to.include("recent-run");
		expect(left).not.to.include("crashed-run");
		stream.end();
	});
});

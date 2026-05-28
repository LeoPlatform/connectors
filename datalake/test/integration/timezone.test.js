'use strict';

// Integration: timezone handling — empirical verification of the rules described in
// ../../CLAUDE.md "Timestamp handling — preserving legacy no-TZ semantics".
//
// Covers:
//   - Session TZ assertion (current_timezone() returns UTC)
//   - Payload-CSV normalization: connector strips trailing `Z` and `±HH:MM`
//     from values destined for TIMESTAMP_NTZ columns, so PERMISSIVE inference
//     sees naked-ISO and preserves the wall-clock
//   - Round-trip parity across canonical timestamp shapes
//   - DST boundary cases — wall-clock preserved, no DST awareness in NTZ
//
// Skips when ~/.databrickscfg [dev-cup] (or env override) is unavailable.

const { Readable } = require('stream');
const { expect } = require('chai');
const { getConfig, checkNonprod } = require('./helpers/databricks.js');
const connectFactory = require('../../lib/connect.js');
const { stagingS3Path } = require('../../lib/connect.js');

let dbconfig;
let client;

before(function() {
	dbconfig = getConfig();
	if (!dbconfig) return this.skip();
	checkNonprod(dbconfig.host);
});

describe('Timezone handling', function() {
	this.timeout(120000);

	before(function() {
		if (!dbconfig) return this.skip();
		client = connectFactory(dbconfig);
	});

	describe('Session parameters (item #5)', function() {
		it('current_timezone() returns UTC', async function() {
			if (!dbconfig) return this.skip();
			const rows = await runQuery(client, 'SELECT current_timezone() AS tz');
			// Databricks normalizes 'UTC' to the IANA alias 'Etc/UTC' on read-back.
			// Both denote the same zero-offset zone — accept either form.
			expect(['UTC', 'Etc/UTC']).to.include(rows[0].tz);
		});
	});

	describe('TIMESTAMP_NTZ parsing via read_files (items #1, #3, #4)', function() {
		// One CSV staged once; all probes read it back. The label column lets each
		// assertion find its row without ordering assumptions.
		const probes = [
			// (#3) Canonical shapes the connector will see in production payloads:
			{ label: 'naked_iso',       ts: '2026-03-15T14:30:00',       note: 'd_order Pacific naive — must preserve wall-clock' },
			{ label: 'z_suffix',        ts: '2026-03-15T14:30:00Z',      note: 'f_item_change_event.last_at UTC Z — see assertion' },
			{ label: 'neg_offset',      ts: '2026-03-15T14:30:00-08:00', note: 'explicit Pacific offset — see assertion' },
			{ label: 'pos_offset',      ts: '2026-03-15T14:30:00+05:30', note: 'explicit IST offset — see assertion' },
			{ label: 'millis_naked',    ts: '2026-03-15T14:30:00.123',   note: 'naked with fractional seconds (Date.toISOString() shape pre-Z-strip)' },
			// (#4) DST boundaries. NTZ has no zone awareness, so these are just wall-clocks.
			{ label: 'dst_spring_skip', ts: '2026-03-08T02:30:00',       note: 'PT spring-forward "skipped" hour — NTZ preserves regardless' },
			{ label: 'dst_fall_amb',   ts: '2026-11-01T01:30:00',        note: 'PT fall-back "ambiguous" hour — NTZ preserves regardless' },
		];

		let resultsByLabel;
		let stagingPath;

		before(async function() {
			if (!dbconfig) return this.skip();
			const result = await stageAndReadProbes(client, dbconfig, probes);
			resultsByLabel = result.rows;
			stagingPath = result.stagingPath;
		});

		after(async function() {
			if (!dbconfig || !stagingPath) return;
			const s3 = new (require('aws-sdk')).S3({ region: dbconfig.region });
			await new Promise((resolve) => {
				s3.deleteObject({
					Bucket: stagingPath.bucket,
					Key: stagingPath.key,
				}, () => resolve());
			});
		});

		it('naked ISO local — preserved as wall-clock', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.naked_iso;
			expect(r.is_null, 'naked ISO local should parse').to.equal(false);
			expect(r.rendered).to.equal('2026-03-15 14:30:00');
		});

		it('naked ISO with fractional seconds — preserved', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.millis_naked;
			expect(r.is_null, 'naked with millis should parse').to.equal(false);
			// We assert on the seconds-rendered form (date_format yyyy-MM-dd HH:mm:ss),
			// so the millis component is implicitly trimmed by the formatter — what
			// matters is that the wall-clock is preserved.
			expect(r.rendered).to.equal('2026-03-15 14:30:00');
		});

		// (#1) Payload-CSV normalization tests. The connector strips trailing `Z`
		// and `±HH:MM` offsets from any value destined for a TIMESTAMP_NTZ column
		// before staging to CSV (see connect.js stripTimestampOffset). The shape
		// reaching read_files is therefore always naked-ISO, regardless of what
		// the producer emitted. These tests assert the wall-clock survives — any
		// drift means either the strip regressed or PERMISSIVE mode silently
		// shifted, both of which would corrupt coexistence with Redshift.
		it('trailing Z — connector strips, wall-clock preserved', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.z_suffix;
			expect(r.is_null, 'Z-suffixed value should normalize and parse, not null').to.equal(false);
			expect(r.rendered, 'Z stripped pre-staging → wall-clock preserved').to.equal('2026-03-15 14:30:00');
		});

		it('explicit negative offset — connector strips, wall-clock preserved', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.neg_offset;
			expect(r.is_null, '-08:00 value should normalize and parse, not null').to.equal(false);
			expect(r.rendered, 'offset stripped pre-staging → wall-clock preserved (no TZ conversion)').to.equal('2026-03-15 14:30:00');
		});

		it('explicit positive offset — connector strips, wall-clock preserved', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.pos_offset;
			expect(r.is_null, '+05:30 value should normalize and parse, not null').to.equal(false);
			expect(r.rendered, 'offset stripped pre-staging → wall-clock preserved (no TZ conversion)').to.equal('2026-03-15 14:30:00');
		});

		// (#4) DST boundary cases. The "skipped" 02:30 doesn't exist in Pacific local
		// time on spring-forward day, and "01:30" on fall-back day is ambiguous in
		// Pacific. With NTZ there is no zone, so both must round-trip as wall-clocks
		// with no exception, no nulling, no correction.
		it('DST spring-forward skipped hour (naked) — preserved as wall-clock', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.dst_spring_skip;
			expect(r.is_null, 'NTZ has no DST awareness — should parse').to.equal(false);
			expect(r.rendered).to.equal('2026-03-08 02:30:00');
		});

		it('DST fall-back ambiguous hour (naked) — preserved as wall-clock', function() {
			if (!dbconfig) return this.skip();
			const r = resultsByLabel.dst_fall_amb;
			expect(r.is_null, 'NTZ has no DST awareness — should parse').to.equal(false);
			expect(r.rendered).to.equal('2026-11-01 01:30:00');
		});
	});
});

// ── helpers ──────────────────────────────────────────────────────────────────

function runQuery(client, sql, params) {
	return new Promise((resolve, reject) => {
		client.query(sql, params || [], (err, rows) => err ? reject(err) : resolve(rows));
	});
}

// Stages the probe rows to S3 as a single CSV via the connector's normal staging
// path, then issues a read_files() SELECT with an explicit TIMESTAMP_NTZ schema,
// rendering each ts via date_format so we can assert on the wall-clock string
// regardless of the underlying type the parser chose. Returns {rows, stagingPath}.
async function stageAndReadProbes(client, dbconfig, probes) {
	const columnDefs = [
		{ name: 'label', type: 'STRING' },
		{ name: 'ts',    type: 'TIMESTAMP_NTZ' },
	];

	// Caller owns the staging-path identifier — same as importFact does in
	// dwconnect.js and as postgres' importFact owns `qualifiedStagingTable`.
	// Probe-prefixed so the file is easy to identify in S3 if a run dies
	// before the after() cleanup runs.
	const auditdate = "'tz_probe_" + Date.now() + "'";
	const stagingPath = stagingS3Path(
		dbconfig.s3Bucket,
		`${dbconfig.s3Prefix}/_tz_probe`,
		'tz_probe',
		auditdate
	);

	const stageStream = client.streamToTableFromS3('tz_probe', {
		columnDefs,
		s3Path: stagingPath,
		region: dbconfig.region,
	});

	await new Promise((resolve, reject) => {
		Readable.from(probes.map(p => ({ label: p.label, ts: p.ts })), { objectMode: true })
			.pipe(stageStream)
			.on('finish', resolve)
			.on('error', reject);
	});

	const stagingSelect = client.buildStagingSelect(stagingPath.uri, columnDefs);
	const wrapper = [
		`SELECT label,`,
		`       date_format(ts, 'yyyy-MM-dd HH:mm:ss') AS rendered,`,
		`       ts IS NULL                              AS is_null`,
		`FROM (`,
		stagingSelect,
		`) AS s`,
	].join('\n');

	const rows = await runQuery(client, wrapper);
	const rowsByLabel = {};
	rows.forEach(r => { rowsByLabel[r.label] = r; });
	return { rows: rowsByLabel, stagingPath };
}

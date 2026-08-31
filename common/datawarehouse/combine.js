const exec = require('child_process').exec;
const fs = require("fs");
const path = require("path");
const PassThrough = require("stream").PassThrough;
const leo = require("leo-sdk");
const ls = leo.streams;
const transform = require("./transform.js");
const combineRecords = require("./combine-records.js");
const async = require("async");
const crypto = require("crypto");

// Field name for the opt-in per-record arrival sequence (see `emitSequence` below).
// Underscore-prefixed to match this warehouse's internal-column convention, so a
// connector's "does this record carry data columns?" checks (which exclude
// `_`-prefixed keys) keep classifying a bare tombstone as data-less.
const SEQUENCE_FIELD = '__leo_seq__';

// Fixed width for an encoded fold-order key (opt-in via `orderFields`, PMT-4302). RStreams eids
// are 40 characters today (z/YYYY/MM/DD/HH/mm/<13-digit ms>-<7-digit seq>); 48 leaves headroom.
// Keys longer than the width are truncated, which is safe for eids because they always differ
// within the first 40.
const ORDER_KEY_WIDTH = 48;

// Encode a fold-order value into a fixed-width sort field. The sort runs under LC_ALL=C, and the
// pad/absent character is a space (0x20), which sorts below every character an eid contains — so
// a row with NO order value (e.g. a backfill row with no source_eid) sorts before every row that
// has one, and a shorter key orders as a strict prefix of a longer one.
function encodeOrderKey(value) {
	let key = value == null ? '' : String(value).replace(/[\r\n]/g, ' ');
	if (key.length > ORDER_KEY_WIDTH) {
		key = key.slice(0, ORDER_KEY_WIDTH);
	}
	return key.padEnd(ORDER_KEY_WIDTH, ' ');
}

module.exports = function(tableIds, opts) {
	let streams = {};
	let count = 0;

	// NOTE: the second argument was previously omitted, so `opts` was silently
	// discarded and `dateFormat` could never be overridden. No in-repo caller passed
	// opts, so restoring it is not a behavior change for existing callers.
	opts = Object.assign({
		dateFormat: d => d.toISOString().slice(0, 19).replace('T', ' '),
		emitSequence: false,
		// Per-table fold ordering (PMT-4302): { [table]: fieldName }. When a table appears here,
		// same-key rows are folded in ascending order of that field's value instead of arrival
		// order, so an out-of-order batch cannot resolve a key to a stale row (current-state
		// tables fed from sharded producers). Default: empty — every table keeps the existing
		// arrival-order fold.
		//
		// KNOWN INTERACTION — delete markers on an ordered table (LeoPlatform/connectors#254):
		// a marker built by checkforDelete carries no order value, so it pads to spaces and
		// sorts FIRST in its group; if a write for the same key shares the batch, combineRecords
		// takes its reactivate branch and the delete is silently dropped — even when the delete
		// genuinely arrived last. Ordered tables are therefore expected NOT to receive queue
		// deletes (the Zero Inventory design routes deletions around the merge path entirely).
		// Order-less rows sorting first is load-bearing and must not be "fixed": it is how a
		// backfill row (no source_eid) loses the fold against live data. A warn is logged when
		// a marker is written for an ordered table.
		orderFields: {}
	}, opts || {});
	let dateFormat = opts.dateFormat;
	// Opt-in (RPL-6780). When on, every record carries the batch-global arrival counter
	// it was assigned here, so a consumer can compare the relative order of two records
	// that combine() placed in *different* natural-key groups — which is the one thing
	// combineRecords' last-event-wins cannot do, because it only ever sees one group.
	// The counter is assigned before grouping, so it reflects true stream arrival order.
	// Default off: with emitSequence false the emitted records are byte-identical to
	// before, so every existing connector (postgres/Redshift included) is unaffected.
	let emitSequence = opts.emitSequence === true;
	let orderFields = opts.orderFields || {};
	let warnedOrderedDeletes = {};

	return ls.through((obj, done) => {
		count++;
		if (count % 10000 == 0) {
			console.log(count, obj.eid);
		}
		let payload = obj.payload;
		let table = transform.parseTable(payload);
		if (table == undefined || tableIds[table] == undefined) {
			return done(null);
		}

		let values = transform.parseValues(payload.data, dateFormat);
		if (emitSequence) {
			// Assigned after parseValues so the field name is never subject to its
			// key-normalization rules.
			values[SEQUENCE_FIELD] = count;
		}

		let stream = streams[table];
		if (!stream) {
			let unsortedFile = `/tmp/leo_dw_${table}`;
			stream = streams[table] = {
				table: table,
				fields: {},
				ordered: !!orderFields[table],
				unsortedFile: unsortedFile,
				sortedFile: unsortedFile + "_sorted",
				stream: fs.createWriteStream(unsortedFile)
			};
		}
		Object.keys(values).forEach(f => stream.fields[f] = 1);
		let id = crypto.createHash('md5');
		id.update(tableIds[table].map(f => values[f]).join(','));

		if (stream.ordered && values.__leo_delete__ && !warnedOrderedDeletes[table]) {
			warnedOrderedDeletes[table] = true;
			console.log(`[combine] WARN: delete marker written for ordered table ${table} — markers carry no ${orderFields[table]} value, sort first in their group, and lose to any same-batch write for the same key (see LeoPlatform/connectors#254)`);
		}

		// Default line: `{32-char md5(nk)}-{9-digit arrival counter}{json}` — the fold keeps the
		// last row per key by arrival. Ordered mode inserts a fixed-width order key between them:
		// `{md5(nk)}-{ORDER_KEY_WIDTH-char order key}-{arrival}{json}` — the fold then keeps the
		// row with the highest order value, and the arrival counter only breaks ties.
		let arrival = ("00000000" + count).slice(-9);
		let sortPrefix = stream.ordered
			? `${id.digest('hex')}-${encodeOrderKey(values[orderFields[table]])}-${arrival}`
			: `${id.digest('hex')}-${arrival}`;

		if (!stream.stream.write(sortPrefix + JSON.stringify(values) + "\n")) {
			stream.stream.once('drain', () => {
				done(null);
			});
		} else {
			done(null);
		}
	}, function(done) {
		let tasks = [];
		let tables = {};

		Object.keys(streams).forEach((t) => {
			tasks.push((done) => {
				let table = streams[t];
				table.stream.end((err) => {
					if (err) {
						return done(err);
					}
					tables[t] = {
						table: t,
						fields: Object.keys(streams[t].fields),
						stream: combine(table.unsortedFile, table.ordered)
					};
					done();
				});
			});
		});
		async.parallelLimit(tasks, 4, err => {
			if (!err) {
				this.push(tables);
			}
			done(err);
		});
	});
};



function combine(file, ordered) {
	let pass = new PassThrough({
		objectMode: true
	});
	file = path.resolve(file);
	console.time("Sorted File " + file);
	var sortedFile = path.resolve(file + "_sorted");
	exec(`sort -S 1G ${file} > ${sortedFile}`, {
		env: {
			LC_ALL: 'C'
		}
	}, function(error) {
		if (error) {
			pass.emit("error", error);
			pass.end();
			return;
		}
		console.timeEnd("Sorted File " + file);
		fs.unlinkSync(file);

		var lastObj = null;
		var lastId = null;
		console.time("Merged File " + sortedFile);
		ls.pipe(fs.createReadStream(sortedFile), ls.split(), ls.through((line, done, push) => {
			try {
				var id = line.substr(0, 32);
				// Default line: json starts after `{32 md5}-{9 arrival}`. Ordered mode inserts
				// `{ORDER_KEY_WIDTH order key}-` between them.
				var data = JSON.parse(line.substr(ordered ? 32 + 1 + ORDER_KEY_WIDTH + 1 + 9 : 42));
			} catch (e) {
				console.log(e);
				console.log(file);
				console.log(line.toString());
				process.exit();
			}
			if (lastObj && id === lastId) {
				// Collapse same-natural-key records in arrival order. A delete is a
				// soft close, so the row's data must survive the collapse — see
				// combine-records.js (RPL-5795).
				lastObj = combineRecords(lastObj, data);
			} else {
				if (lastObj) {
					push(lastObj);
				}
				lastObj = data;
			}
			lastId = id;
			done();
		},
		function(done) {
			if (lastObj) {
				done(null, lastObj);
			}
		}), pass, (err) => {
			if (err) {
				pass.emit('error', err);
			} else {
				console.timeEnd("Merged File " + sortedFile);
				fs.unlinkSync(sortedFile);
			}
		});
	});
	return pass;
}

module.exports.SEQUENCE_FIELD = SEQUENCE_FIELD;

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

module.exports = function(tableIds, opts) {
	let streams = {};
	let count = 0;

	// NOTE: the second argument was previously omitted, so `opts` was silently
	// discarded and `dateFormat` could never be overridden. No in-repo caller passed
	// opts, so restoring it is not a behavior change for existing callers.
	opts = Object.assign({
		dateFormat: d => d.toISOString().slice(0, 19).replace('T', ' '),
		emitSequence: false
	}, opts || {});
	let dateFormat = opts.dateFormat;
	// Opt-in. When on, every record carries the batch-global arrival counter
	// it was assigned here, so a consumer can compare the relative order of two records
	// that combine() placed in *different* natural-key groups — which is the one thing
	// combineRecords' last-event-wins cannot do, because it only ever sees one group.
	// The counter is assigned before grouping, so it reflects true stream arrival order.
	// Default off: with emitSequence false the emitted records are byte-identical to
	// before, so every existing connector (postgres/Redshift included) is unaffected.
	let emitSequence = opts.emitSequence === true;

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
				unsortedFile: unsortedFile,
				sortedFile: unsortedFile + "_sorted",
				stream: fs.createWriteStream(unsortedFile)
			};
		}
		Object.keys(values).forEach(f => stream.fields[f] = 1);
		let id = crypto.createHash('md5');
		id.update(tableIds[table].map(f => values[f]).join(','));

		if (!stream.stream.write(`${id.digest('hex')}-${("00000000"+count).slice(-9)}` + JSON.stringify(values) + "\n")) {
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
						stream: combine(table.unsortedFile)
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



function combine(file) {
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
				var data = JSON.parse(line.substr(42));
			} catch (e) {
				console.log(e);
				console.log(file);
				console.log(line.toString());
				process.exit();
			}
			if (lastObj && id === lastId) {
				// Collapse same-natural-key records in arrival order. A delete is a
				// soft close, so the row's data must survive the collapse — see
				// combine-records.js.
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

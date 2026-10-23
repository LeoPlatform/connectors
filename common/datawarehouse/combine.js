const exec = require('child_process').exec;
const fs = require("fs");
const path = require("path");
const merge = require("lodash/merge");
const PassThrough = require("stream").PassThrough;
const leo = require("leo-sdk");
const ls = leo.streams;
const transform = require("./transform.js");
const async = require("async");
const crypto = require("crypto");

// A pipeline directory nothing has written to for this long belongs to a run that has ended: Lambda stops an
// invocation after at most 15 minutes.
const STALE_DIR_MS = 15 * 60 * 1000;

module.exports = function(tableIds, opts) {
	let streams = {};
	let count = 0;

	// Each pipeline buffers its tables in its own directory. A warm Lambda container can still be running an earlier
	// invocation's pipeline when the next one starts, and with fixed per-table paths the two overwrote, sorted and
	// unlinked each other's files (ES-5648).
	let root = process.env.LEO_DW_TMP_DIR || "/tmp/leo_dw";
	removeStaleDirs(root);
	let dir = path.join(root, crypto.randomBytes(8).toString("hex"));
	fs.mkdirSync(dir, { recursive: true });

	opts = Object.assign({
		dateFormat: d => d.toISOString().slice(0, 19).replace('T', ' ')
	});
	let dateFormat = opts.dateFormat;

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

		let stream = streams[table];
		if (!stream) {
			let unsortedFile = path.join(dir, table);
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

		// Remove this pipeline's directory once every table's sort and merge has finished, whether or not it succeeded.
		let remaining = Object.keys(streams).length;
		let finished = () => {
			if (--remaining === 0) {
				fs.rmSync(dir, { recursive: true, force: true });
			}
		};
		if (remaining === 0) {
			fs.rmSync(dir, { recursive: true, force: true });
		}

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
						stream: combine(table.unsortedFile, finished)
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



function combine(file, finished) {
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
			finished();
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
				if (data.__leo_delete__ || lastObj.__leo_delete__) {
					// if our current row is a delete, then we throw away the previous updates
					// if our previous row was a delete, but the current one is not, we throw away the delete, because it was updated
					// after the delete, so it should NOT be deleted.
					lastObj = data;
				} else {
					lastObj = merge(lastObj, data);
				}
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
				finished();
				pass.emit('error', err);
			} else {
				console.timeEnd("Merged File " + sortedFile);
				fs.unlinkSync(sortedFile);
				finished();
			}
		});
	});
	return pass;
}

// Removes pipeline directories under root that nothing has written to for STALE_DIR_MS: ones left by a run that
// crashed, or by an earlier process in this container.
function removeStaleDirs(root) {
	let names;
	try {
		names = fs.readdirSync(root);
	} catch (err) {
		return; // no pipeline has run here yet
	}
	let cutoff = Date.now() - STALE_DIR_MS;
	names.forEach(name => {
		let dir = path.join(root, name);
		try {
			let lastWrite = fs.readdirSync(dir).reduce(
				(latest, file) => Math.max(latest, fs.statSync(path.join(dir, file)).mtimeMs),
				fs.statSync(dir).mtimeMs
			);
			if (lastWrite < cutoff) {
				fs.rmSync(dir, { recursive: true, force: true });
			}
		} catch (err) {
			// Its pipeline removed it, or a file in it, while we looked.
		}
	});
}

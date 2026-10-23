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

// Directories of pipelines still alive in this process. The stale sweep never removes these: a failed run's
// pipeline resumes in the next invocation after the container sat frozen, so its files can look old while it
// still needs them (prod: merges of up to 8.7 min with no file change, containers idle up to 10.7 min).
const liveDirs = new Set();
// A directory no live pipeline here owns, untouched for this long, is left by a crashed run or an earlier
// process. The age check is for hosts where other processes share /tmp.
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
	liveDirs.add(dir);
	let flushStarted = false;

	opts = Object.assign({
		dateFormat: d => d.toISOString().slice(0, 19).replace('T', ' ')
	});
	let dateFormat = opts.dateFormat;

	let combiner = ls.through((obj, done) => {
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
		flushStarted = true;
		let tasks = [];
		let tables = {};

		// Remove this pipeline's directory once every table's sort and merge has finished, whether or not it succeeded.
		let remaining = Object.keys(streams).length;
		let finished = () => {
			if (--remaining === 0) {
				removeDir(dir);
			}
		};
		if (remaining === 0) {
			removeDir(dir);
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

	// Torn down before it sorted anything (an upstream error destroys every stream in the pipe): nothing else will
	// remove the directory, and it would stay live for the life of the process. Close the table files first, or a
	// write still in flight recreates a file while the directory is being removed.
	combiner.on("close", () => {
		if (flushStarted) {
			return;
		}
		let open = Object.keys(streams).map(t => streams[t].stream).filter(file => !file.closed);
		let pending = open.length;
		if (!pending) {
			return removeDir(dir);
		}
		open.forEach(file => {
			file.once("close", () => {
				if (--pending === 0) {
					removeDir(dir);
				}
			});
			file.destroy();
		});
	});
	return combiner;
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

function removeDir(dir) {
	liveDirs.delete(dir);
	try {
		fs.rmSync(dir, { recursive: true, force: true });
	} catch (err) {
		// Cleanup must never fail a run. No longer live, so the stale sweep removes it later.
		console.log(`Could not remove ${dir}: ${err.message}`);
	}
}

// Removes pipeline directories under root that no live pipeline in this process owns and that nothing has written
// to for STALE_DIR_MS: ones left by a run that crashed, or by an earlier process in this container.
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
		if (liveDirs.has(dir)) {
			return;
		}
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

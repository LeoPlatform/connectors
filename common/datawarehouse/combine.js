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

module.exports = function(tableIds, opts) {
	let streams = {};
	let count = 0;

	opts = Object.assign({
		dateFormat: d => d.toISOString().slice(0, 19).replace('T', ' ')
	});
	let dateFormat = opts.dateFormat;

	return ls.through((obj, done) => {
		count++;
		if (count % 10000 == 0) {
			console.log(count);
		}
		let payload = obj.payload;
		let table = transform.parseTable(payload);
		if (table == undefined || tableIds[table] == undefined) {
			return done(null);
		}

		let values = transform.parseValues(payload.data, dateFormat);

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
					lastObj = merge(lastObj, data);
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
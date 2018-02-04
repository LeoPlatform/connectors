const exec = require('child_process').exec;
const fs = require("fs");
const path = require("path");
const merge = require("lodash/merge");
const readline = require('readline');
const PassThrough = require("stream").PassThrough;
const leo = require("leo-sdk");
const ls = leo.streams;
const transform = require("./transform.js");
const async = require("async");


module.exports = function(toStream) {
	let streams = {};
	let count = 0;
	let mergeCount = 0;
	let tableLoadCounts = {};

	let fields = {};
	return ls.write((obj, enc, done) => {
		let payload = obj.payload;

		let table = transform.parseTable(payload);
		let values = transform.parseValues(payload.data);

		Object.keys(values).forEach(f => fields[f] = 1);

		let stream = streams[table];
		if (!stream) {
			let unsortedFile = `/tmp/leo_dw_${table}`;
			stream = streams[table] = {
				table: table,
				unsortedFile: unsortedFile,
				sortedFile: unsortedFile + "_sorted",
				stream: fs.createWriteStream(unsortedFile)
			};
		}
		count++;
		if (count % 10000 == 0) {
			console.log(count);
		}
		if (!stream.stream.write(`${values.id}-${("00000000"+count).slice(-9)}|` + JSON.stringify(values) + "\n")) {
			stream.stream.once('drain', () => {
				done(null);
			});
		} else {
			done(null);
		}
	}, function(done) {
		tasks = [];
		Object.keys(streams).forEach((t) => {
			tasks.push((done) => {
				let table = streams[t];
				table.stream.end((err) => {
					if (err) {
						return done(err);
					}
					ls.pipe(combine(table.unsortedFile), ls.through((obj, done) => {
						tableLoadCounts[t]++;
						mergeCount++;
						done(null, obj);
					}), toStream(table.table, Object.keys(fields)), (err) => {
						console.log("done here");
						done();
					});
				});
			});
		});
		async.parallelLimit(tasks, 4, err => {
			console.log(err);
			console.log("all done", done);
			done();
		})
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
	}, function(error, stdout, stderr) {
		if (error) {
			pass.emit("err", error);
			pass.end();
			return;
		}
		console.timeEnd("Sorted File " + file);
		fs.unlinkSync(file);

		var lastObj = null;
		console.time("Merged File " + sortedFile);
		ls.pipe(fs.createReadStream(sortedFile), ls.split(), ls.through((line, done, push) => {
			try {
				var data = JSON.parse(line.replace(/^.*?\|{/, '{'));
			} catch (e) {
				console.log(e);
				console.log(file);
				console.log(line.toString());
				process.exit();
			}
			if (lastObj && data.id == lastObj.id) { //we want double equals here not triple, becuase 1 and "1" should be the same
				lastObj = merge(lastObj, data);
			} else {
				if (lastObj) {
					push(lastObj);
				}
				lastObj = data;
			}
			done();
		}, function(done) {
			if (lastObj) {
				done(null, lastObj);
			}
		}), pass, (err) => {
			if (err) {
				console.log(err);
				pass.emit('err', err);
			} else {
				console.timeEnd("Merged File " + sortedFile);
				// fs.unlinkSync(sortedFile);
			}
		});
	});
	return pass;
}
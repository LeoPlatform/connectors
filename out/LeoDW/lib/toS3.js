const leo = require("leo-sdk");
const ls = leo.streams;
const transform = require("./transform.js");
const fs = require("fs");
const async = require("async");
const combine = require("./combine.js");

module.exports = {
	streams: {},
	through: function(flush) {
		this.streams = {};
		let count = 0;
		let mergeCount = 0;
		let tableLoadCounts = {};

		let fields = {};
		return ls.through((obj, done) => {
			console.log
			let payload = obj.payload;

			let table = transform.parseTable(payload);
			let values = transform.parseValues(payload.data);

			Object.keys(values).forEach(f => fields[f] = 1);

			let stream = this.streams[table];
			if (!stream) {
				let unsortedFile = `/tmp/leo_dw_${table}`;
				stream = this.streams[table] = {
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
					done(null, obj);
				});
			} else {
				done(null, obj);
			}
		}, (done) => {
			tasks = [];
			Object.keys(this.streams).forEach((t) => {
				tasks.push((done) => {
					let table = this.streams[t];
					table.stream.end((err) => {
						if (err) {
							return done(err);
						}
						ls.pipe(combine(table.unsortedFile), ls.through((obj, done) => {
							tableLoadCounts[t]++;
							mergeCount++;
							done(null, obj);
						}), ls.toCSV(Object.keys(fields), {
							trim: true,
							escape: '\\',
							nullValue: "\\N",
							delimiter: '|'
						}), fs.createWriteStream('/tmp/new_cool_file.csv'), (err) => {
							if (err) {
								done(err);
							} else {
								done();
							}
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
	}
};
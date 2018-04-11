const leo = require("leo-sdk");
const ls = leo.streams;

const nibbler = require("./nibbler.js");
const loader = require("./loader.js");
const loaderJoin = require("./loaderJoinTable.js");

const moment = require("moment");

let dynamodb = leo.aws.dynamodb;
const tableName = leo.configuration.resources.LeoCron;

module.exports = function(botId, client, table, id, domain, opts, callback) {
	opts = Object.assign({
		event: table,
	}, opts || {});

	let nibble = null;
	var logTimeout = null;
	//@todo: Update all this to use the log-update node module
	function clearLog() {
		process.stdout.write("\r\x1b[K");
		if (logTimeout) clearInterval(logTimeout);
	}
	var log = function() {
		clearLog();
		var percent = (nibble.progress / nibble.total) * 100;
		var fixed = percent.toFixed(2);
		if (fixed == "100.00" && percent < 100) {
			fixed = "99.99";
		}
		console.log(fixed + "% :", Object.keys(arguments).map(k => arguments[k]).join(", "));
	};

	function timeLog(message) {
		clearLog();
		var time = new Date();

		function writeMessage() {
			process.stdout.write("\r\x1b[K");
			process.stdout.write(((new Date() - time) / 1000).toFixed(1) + "s : " + message);
		}
		writeMessage();
		logTimeout = setInterval(writeMessage, 200);
	}

	function normalLog(message) {
		clearLog();
		console.log(message);
	}


	function saveProgress(data, timestamp) {
		dynamodb.merge(tableName, botId, {
			snapshot: Object.assign({
				last_run: moment.now(),
				bucket_timestamp: timestamp && timestamp.valueOf()
			}, data)
		}, function(err, result) {
			if (err) {
				console.log(err);
				callback(err);
				process.exit();
			}
		});
	}
	dynamodb.get(tableName, botId, function(err, result) {
		if (err) {
			callback(err);
		} else {
			// reuse an existing bucket key if we’re resuming, otherwise create a new one.
			let timestamp = moment(result && result.snapshot && !result.snapshot.complete && result.snapshot.bucket_timestamp || undefined),
				bucketKey = timestamp.format('YYYY/MM_DD_') + timestamp.valueOf();

			let stream = nibbler(client, table, id, {
				limit: 5000,
				resume: result && result.snapshot && !result.snapshot.complete && result.snapshot
			});
			stream.destroy = stream.destroy || stream.close || (() => {});

			stream.on("ranged", function(n) {
				nibble = n;
				saveProgress(nibble, timestamp);
			});
			let transform;
			if (Array.isArray(id)) {
				transform = loaderJoin(client, id, null, domain, {
					source: 'snapshot',
					isSnapshot: true
				});
			} else {
				transform = loader(client, (obj) => {
					return [obj[table]];
				}, domain, {
					source: 'snapshot',
					isSnapshot: true
				});
			}
			transform.destroy = transform.destroy || transform.close || (() => {});

			let timeout = setTimeout(() => {
				stream.stop();
			}, moment.duration({
				seconds: 180
			}).asMilliseconds());

			ls.pipe(stream,
				transform,
				ls.toS3GzipChunks(opts.event, {
					useS3Mode: true,
					time: {
						minutes: 1
					},
					prefix: "_snapshot/" + bucketKey,
					sectionCount: 30
				}),
				ls.toLeo("snapshotter", {
					snapshot: timestamp.valueOf()
				}),
				ls.cmd("checkpoint", (obj, done) => {
					if (obj.correlations) {
						let records = 0;
						obj.correlations.forEach(c => {
							nibble.start = c.snapshot.end;
							nibble.progress += c.snapshot.records;
							records += c.snapshot.records;
						});
						log(`Processed  ${records}  ${nibble.progress}/${nibble.total}. Remaining ${nibble.total-nibble.progress}`);
						if (nibble.end == nibble.start) {
							nibble.complete = true;
							saveProgress(nibble);
							let closeStream = ls.pipeline(ls.toLeo("snapshotter", {
								snapshot: timestamp.valueOf()
							}), ls.devnull());
							closeStream.write({
								_cmd: 'registerSnapshot',
								event: opts.event,
								start: timestamp.valueOf(),
								next: timestamp.clone().startOf('day').valueOf(),
								id: botId
							});
							closeStream.on("finish", done);
							closeStream.end();
						} else {
							saveProgress(nibble, timestamp);
							done();
						}
					} else {
						done();
					}
				}),
				ls.devnull(),
				(err) => {
					clearTimeout(timeout);
					if (err) {
						console.log(err);
						stream.destroy();
						transform.destroy();
						callback(err);
					} else {
						if (!nibble.complete) {
							leo.bot.runAgain();
						}
						callback(null);
					}
				});
		}
	});
};

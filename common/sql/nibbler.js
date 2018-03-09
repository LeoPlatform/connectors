let PassThrough = require("stream").PassThrough;
const async = require('async');

module.exports = function(client, table, id, opts) {
	opts = Object.assign({
		time: 1,
		limit: 20000,
		maxLimit: 1000000,
		reverse: true
	}, opts || {});

	var nibble = {};
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

	let pass = new PassThrough({
		objectMode: true
	});
	client.range(table, id, null, (err, result) => {
		//Now let's nibble our way through it.
		nibble = {
			start: (opts.start && (opts.start < result.max || opts.start > result.min)) ? opts.start : opts.reverse ? result.max : result.min,
			end: opts.reverse ? result.min : result.max,
			limit: opts.limit,
			next: null,
			max: result.max,
			min: result.min,
			total: result.total,
			progress: 0,
			reverse: opts.reverse
		};

		log(`Starting.  Total: ${nibble.total}`);
		//var hadRecentErrors = 0;
		async.doWhilst(done => {
				let sql;
				let params;
				client.nibble(table, id, nibble.start, nibble.min, nibble.max, nibble.limit, opts.reverse, (err, result) => {
					if (err) {
						return done(err);
					}
					if (!result[0]) {
						nibble.end = opts.reverse ? nibble.min : nibble.max;
						nibble.next = null;
					} else if (!result[1]) {
						nibble.end = result[0].id;
						nibble.next = null;
					} else {
						nibble.end = result[0].id;
						nibble.next = result[1].id;
					}

					client.getIds(table, id, nibble.start, nibble.end, opts.reverse, (err, result) => {
						if (err) {
							return done(err);
						}
						nibble.start = nibble.next;
						let ids = result.map(r => r.id);
						if (!pass.write({
								payload: {
									[table]: ids
								},
								nibble
							})) {
							pass.once('drain', done);
						} else {
							done();
						}
					});
				});
			},
			() => {
				return nibble.start != null;
			},
			(err, data) => {
				console.log(err);
				if (err) {
					pass.emit("error", err);
				} else {
					pass.end();
				}
			});
	});
	return pass;
};

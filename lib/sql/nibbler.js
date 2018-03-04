let PassThrough = require("stream").PassThrough;
const async = require('async');

module.exports = function(client, table, id, opts) {
	var opts = Object.assign({
		time: 1,
		limit: 20000,
		maxLimit: 1000000,
		reverse: false
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
	client.query(`select min(??) as min, max(??) as max, count(??) as total from ??`, [id, id, id, table], (err, result) => {
		console.log(err, result);
		//Now let's nibble our way through it.
		nibble = {
			start: result[0].min,
			end: result[0].max,
			limit: opts.limit,
			next: null,
			max: result[0].max,
			min: result[0].min,
			total: result[0].total,
			progress: 0,
			reverse: opts.reverse,
			move: function() {
				if (!this.reverse)
					this.start = this.next;
				else
					this.end = this.next;
			}
		};

		log(`Starting.  Total: ${nibble.total}`);
		//var hadRecentErrors = 0;
		async.doWhilst(done => {
				console.log(nibble);
				client.query(`select ?? as id from ??  
					where ?? >= ? and ?? <= ?
					ORDER BY ?? ${opts.reverse?'desc':'asc'}
					LIMIT ${nibble.limit-1},2
					`, [id, table, id, nibble.start, id, nibble.max, id], (err, result) => {
					if (err) {
						return done(err);
					}
					console.log(result);
					if (!result[0]) {
						nibble.end = nibble.max;
						nibble.next = null;
					} else if (!result[1]) {
						nibble.end = result[0].id;
						nibble.next = null;
					} else {
						nibble.end = result[0].id;
						nibble.next = result[1].id;
					}

					client.query(`select ?? as id from ??  
					where ?? >= ? and ?? <= ?
					ORDER BY ?? ${opts.reverse?'desc':'asc'}
					LIMIT ${nibble.limit}
					`, [id, table, id, nibble.start, id, nibble.end, id], (err, result) => {
						if (err) {
							return done(err);
						}
						nibble.start = nibble.next;

						let ids = result.map(r => r.id);

						if (!pass.write({
								payload: {
									[table]: ids
								}
							})) {
							pass.once('drain', done);
						} else {
							done();
						}
					});
				});
			},
			() => {
				return !nibble.reverse ? nibble.start != null : nibble.end != null;
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

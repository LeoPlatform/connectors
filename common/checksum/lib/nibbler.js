const async = require("async");
const logger = require("leo-logger")("leo-nibbler");

/**
 * Interface
 * nibbler.sync({
 *      onInit: function(nibble) { }, // setup stuff for sync; Async is not allowed
 *      whilst: function(nibble) { return true; }, // return true to continue;  Async is not allowed
 *      onBite: function(nibble, done) { done(); }, // do something with the nibble data
 *      onError: function(err, data, nibble, done) { nibble.limit = nibble.limit / 2; done(); }, // Fix the error by addressing nibble settings or nibble.move() or settings to end
 *      onEnd: function(err, nibble, done) { done(); }
 *
 * }, callback);
 */

module.exports = function(connector, opts) {
	var nibble = {};

	var logTimeout = null;
	//@todo: Update all this to use the log-update node module
	function clearLog() {
		if (opts.showOutput) {
			process.stdout.write("\r\x1b[K");
		}
		if (logTimeout) clearInterval(logTimeout);
	}
	var log = function() {
		clearLog();
		var percent = (nibble.progress / nibble.total) * 100;
		var fixed = percent.toFixed(2);

		if (fixed == "100.00" && percent < 100) {
			fixed = "99.99";
		}

		logger.log(fixed + "% :", Object.keys(arguments).map(k => arguments[k]).join(", "));
	}

	function timeLog(message) {
		clearLog();
		var time = new Date();

		function writeMessage() {
			if (opts.showOutput) {
				process.stdout.write("\r\x1b[K");
				process.stdout.write(((new Date() - time) / 1000).toFixed(1) + "s : " + message);
			} else {
				logger.log(message);
			}
		}
		writeMessage();

		if (opts.showOutput) {
			logTimeout = setInterval(writeMessage, 200);
		}
	}

	function normalLog(message) {
		clearLog();
		logger.log(message);
	}

	return {
		log: log,
		timeLog: timeLog,
		normalLog: normalLog,
		sync: function(opts, callback) {
			if (typeof opts == "function") {
				callback = opts;
				opts = {};
			}
			var opts = Object.assign({
				time: 1,
				limit: 9000,
				maxLimit: 1000000,
				sample: false,
				reverse: false,
			}, opts || {});

			var cb = callback;
			callback = (err, data) => {
				clearLog();
				cb(err, data);
			}
			connector.range(opts).then((range) => {
				// logger.log("nibbler.sync", JSON.stringify(opts, null, 2))
				//Now let's nibble our way through it.
				nibble = {
					start: opts.start && opts.start > range.min ? opts.start : range.min,
					end: opts.end && opts.end < range.max ? opts.end : range.max,
					limit: opts.limit,
					next: null,
					min: range.min,
					max: range.max,
					total: range.total,
					progress: 0,
					reverse: opts.reverse,
					hadRecentErrors: 0,
					move: function() {
						if (!this.reverse)
							this.start = this.next;
						else
							this.end = this.next;
					}
				};

				if (opts.onInit) {
					opts.onInit(nibble);
				}

				log(`Starting.  Total: ${nibble.total}, Min: ${nibble.min}, Max: ${nibble.max}, Start: ${nibble.start}, End: ${nibble.end}`);
				//var hadRecentErrors = 0;
				async.doWhilst((done) => {
						timeLog("Nibbling the next set of Data from Locale");
						var forward = !nibble.reverse;
						nibble.start = forward ? nibble.start : nibble.min;
						nibble.end = forward ? nibble.max : nibble.end;

						connector.nibble(nibble).then((n) => {
							if (n.current != undefined) {
								if (!nibble.reverse) {
									n.end = n.current ? n.current : nibble.max;
								} else {
									n.start = n.current ? n.current : nibble.min;
								}
								delete n.current;
							}
							Object.assign(nibble, n);

							opts.onBite(nibble, (err, result) => {
								if (err) {
									opts.onError(err, result, nibble, done);
									nibble.hadRecentErrors = opts.errorAllowance || 1;
								} else {
									nibble.move();

									//we had no errors this time nor last time, so lets up the limit
									if (nibble.hadRecentErrors) {
										nibble.hadRecentErrors--;
									} else {
										nibble.limit = Math.min(
											nibble.limit * 5,
											opts.maxLimit,
											(result && result.duration) ? (Math.round((1000 / Math.max(result.duration)) * nibble.limit)) : opts.maxLimit
										);
									}
									done(null, result);
								}

							});
						}, callback);
					},
					function() {
						return (!nibble.reverse ? nibble.start != null : nibble.end != null) && (!opts.whilst || opts.whilst(nibble));
					},
					function(err, data) {
						//clearLog();
						if (opts.onEnd) {
							opts.onEnd(err, nibble, function(err2, data) {
								callback(err || err2, data);
							});
						} else {
							callback(err);
						}
					});
			}, callback).catch(callback);
		}
	};
};
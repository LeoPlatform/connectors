module.exports = function (checksumlib) {
	return {
		handler: function (event, context, callback) {
			context.callbackWaitsForEmptyEventLoop = false;
			var idColumn = event.id_column;
			var method = event.params.querystring.method;

			if (method == "batch") {
				delete event.body.checksum;
				checksumlib.batch(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "individual") {
				delete event.body.individual;
				checksumlib.individual(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "delete") {
				delete event.body.delete;
				checksumlib.delete(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "sample") {
				delete event.body.sample;
				checksumlib.sample(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "nibble") {
				delete event.body.nibble;
				checksumlib.nibble(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "range") {
				delete event.body.range;
				checksumlib.range(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "initialize") {
				delete event.body.initialize;
				checksumlib.initialize(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else if (method == "destroy") {
				delete event.body.destroy;
				checksumlib.destroy(event.body, (err, data) => {
					callback(err, {
						response: data,
						session: event.body.session
					});
				});
			} else {
				callback("Invalid Method: " + method)
			}
		}
	};
};
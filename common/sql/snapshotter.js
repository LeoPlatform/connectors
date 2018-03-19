const leo = require("leo-sdk");
const ls = leo.streams;

const nibbler = require("./nibbler.js");
const loader = require("./loader.js");

const moment = require("moment");

module.exports = function(botId, client, table, id, domain, opts, callback) {
	opts = Object.assign({
		event: table,
	}, opts || {});

	let stream = nibbler(client, table, id, {
		limit: 5000
	});
	let transform = loader(client, {
		[table]: true
	}, domain, {
		source: 'snapshot'
	});

	let timestamp = moment();
	ls.pipe(stream, transform, ls.toS3GzipChunks(opts.event, {
			useS3Mode: true,
			time: {
				minutes: 1
			},
			prefix: "_snapshot/" + timestamp.format("YYYY/MM_DD_") + timestamp.valueOf(),
			sectionCount: 30
		}, function(done, push) {
			push({
				_cmd: 'registerSnapshot',
				event: opts.event,
				start: timestamp.valueOf(),
				next: timestamp.clone().startOf('day').valueOf(),
				id: botId
			});
			done();
		}),
		ls.toLeo("snapshotter", {
			snapshot: timestamp.valueOf()
		}), (err) => {
			if (err) {
				console.log(err);
				stream.destroy();
				transform.destroy();
			}
			callback(err);
		});
};

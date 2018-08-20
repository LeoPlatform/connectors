'use strict';
const leolistener = require("./listener");
const leo = require('leo-sdk');
const ls = leo.streams;
const logger = require('leo-logger')('leo-connector-mysql/lib/streamchanges');
let counter = 0;

module.exports = function (connection, tables, opts = {}) {
	// make sure we have a start
	opts.start = opts.start || '0::0';
	let start = opts.start.split('::');

	// Client code
	// @todo change connection from config to an actual connection
	let listener = new leolistener(connection);

	// by default, pass in 100 records or whatever we get in 15 seconds
	let stream = ls.batch(opts.batch || {
		count: 100,
		time: {
			seconds: 15
		}
	});

	listener.on('binlog', function (event) {
		if (event.writerows) {
			event = event.writerows;
			streamWrite(event, 'update', stream);
		} else if (event.updaterows) {
			event = event.updaterows;
			event.data = event.data.after;

			streamWrite(event, 'update', stream);
		} else if (event.deleterows) {
			event = event.deleterows;
			streamWrite(event, 'delete', stream);
			// } else if (event.rotate) {
		}
	});

	// catch errors and try to continue
	listener.on('error', function(err) {
		logger.error('[Error]', err);
	});

	let includeSchema = {};
	if (tables) {
		// log only selected tables
		includeSchema[opts.config.database] = Object.keys(tables);
	} else {
		// log all table changes for the selected database
		includeSchema[opts.config.database] = true;
	}

	listener.start({
		binlogName: start[0] || undefined,
		binlogNextPos: parseInt(start[1]) > 3 && start[1] || undefined,
		serverId: 1,
		includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'],
		includeSchema: includeSchema,
	});

	process.on('SIGINT', function () {
		console.log('Got SIGINT.');
		// end the mysql listener
		listener.stop();

		// end the stream
		stream.end(err => {
			console.log("All done loading events", err);
		});
		process.exit();
	});

	// let ids = 'vehicle_id';
	return ls.pipeline(stream
		, ls.through((batch, done) => {
			let payload = {
				update: {},
				delete: {}
			};

			batch.correlation_id = {
				start: '',
				end: '',
				source: opts.source || 'system:mysql'
			};

			batch.payload.forEach(row => {
				let type = (row.delete) ? 'delete' : 'update';

				for (let table in row[type]) {
					let eid = row[type][table].binlogName + '::' + row[type][table].binlogNextPos;

					// set the correlation data
					// set start only if it doesn't exist (it'll be our first result)
					if (!batch.correlation_id.start) {
						batch.correlation_id.start = eid;
					}

					// always set the end. It'll end up being the last one in the batch.
					batch.correlation_id.end = eid;
					logger.info('eid', eid);

					// set the payload
					if (!payload[type][table]) {
						payload[type][table] = [];
					}

					// tables[table] will be the either a single PK or a Composite Key
					if (tables[table]) {
						if (typeof tables[table] === 'string') {
							payload[type][table].push(row[type][table].payload[tables[table]]);
						} else {
							// handle composite keys
							let tmp = {};
							for (let id of tables[table]) {
								tmp[id] = row[type][table].payload[id];
							}

							payload[type][table].push(tmp);
						}
					}
				}
			});

			batch.payload = payload;

			done(null, batch);
		})
	);
};

/**
 * Write to the stream
 * @param event
 * @param type
 * @param stream
 */
function streamWrite(event, type, stream) {
	let writeEvent = {};
	writeEvent[type] = {};
	writeEvent[type][event.tableName] = {
		payload: event.data,
		binlogName: event.binlogName,
		binlogNextPos: event.binlogNextPos,
		timestamp: event.timestamp,
	};

	stream.write(writeEvent);
}

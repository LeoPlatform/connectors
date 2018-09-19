'use strict';

const ZongJi = require("zongji");
const ls = require('leo-streams');
const moment = require("moment");
const logger = require("leo-logger")("mysql").sub("binloglistener");
logger.configure(/mysql\.binloglistener/, {
	info: true
});

let typeMap = {
	writerows: 'update',
	updaterows: 'update',
	deleterows: 'delete'
};

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
	var self = this;
	var sql = `SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
		FROM information_schema.columns WHERE table_schema='${tableMapEvent.schemaName}' 
		AND table_name='${tableMapEvent.tableName}'`;

	if (self.__ended) {
		return;
	}

	this.ctrlConnection.query(sql, function(err, rows) {
		if (err) {
			// Errors should be emitted
			self.emit('error', err);
			// This is a fatal error, no additional binlog events will be
			// processed since next() will never be called
			return;
		}
		if (rows.length === 0) {
			self.emit('error', new Error(
				'Insufficient permissions to access: ' +
				tableMapEvent.schemaName + '.' + tableMapEvent.tableName));
			// This is a fatal error, no additional binlog events will be
			// processed since next() will never be called
			return;
		}
		self.tableMap[tableMapEvent.tableId] = {
			columnSchemas: rows,
			parentSchema: tableMapEvent.schemaName,
			tableName: tableMapEvent.tableName,
			primaryKey: rows.filter(r => r.COLUMN_KEY == "PRI").map(r => r.COLUMN_NAME)
		};

		next();
	});
};

ZongJi.prototype.start = function(options) {
	var self = this;
	self.set(options);

	let pass = ls.passthrough({
		objectMode: true
	});

	// Pass along any errors from the connection
	self.on("error", error => pass.emit("error", error));

	let passEnd = pass.end;
	pass.end = function() {
		self.__ended = true;
		self.connection.destroy();

		logger.debug("Ending binglog stream");
		self.ctrlConnection.query(
			'KILL ' + self.connection.threadId,
			function() {
				if (self.ctrlConnectionOwner) {
					self.ctrlConnection.destroy();
				}
				passEnd.call(pass);
			}
		);
	};

	var _start = function() {
		self.connection._implyConnect();
		if (self.options.binlogName) {
			logger.log(`Starting from ${self.options.source} ${self.options.binlogName}::${self.options.binlogNextPos}`);
		} else {
			logger.log(`Starting from ${self.options.source} latest`);
		}
		self.connection._protocol._enqueue(new self.binlog(function(error, event) {
			if (error) return self.emit('error', error);
			// Do not emit events that have been filtered out
			if (event === undefined || event._filtered === true) return;


			let eventName = event.getTypeName().toLowerCase();
			switch (eventName) {
				case 'tablemap':
					var tableMap = self.tableMap[event.tableId];
					if (!tableMap) {
						self.connection.pause();
						self._fetchTableInfo(event, function() {
							// merge the column info with metadata
							event.updateColumnInfo();
							self.connection.resume();
						});
					}
					return;
					break;
				case 'rotate':
					if (self.binlogName !== event.binlogName) {
						self.binlogName = event.binlogName;
					}
					return;
					break;
			}
			self.binlogNextPos = event.nextPosition;

			let tableName = event.tableMap[event.tableId].tableName;
			let schema = event.tableMap[event.tableId].parentSchema;
			if (event.tableMap[event.tableId].primaryKey.length && !pass.write({
					timestamp: event.timestamp,
					event_source_timestamp: event.timestamp,
					correlation_id: {
						source: self.options.source || 'system:mysql',
						start: self.binlogName + "::" + event.nextPosition
					},
					payload: {
						schema: schema,
						tableName: tableName,
						primaryKey: event.tableMap[event.tableId].primaryKey,
						type: typeMap[eventName],
						rows: event.rows
					}
				})) {
				self.connection.pause();
				pass.once('drain', () => {
					self.connection.resume();
				});
			}
		}));
	};
	if (this.ready) {
		_start();
	} else {
		this.ctrlCallbacks.push(_start);
	}

	return pass;
};

module.exports = function(config, opts) {
	opts = Object.assign({
		server_id: 99999,
		includeSchema: undefined,
		excludeSchema: {
			information_schema: true,
			mysql: true,
			performance_schema: true,
			sys: true
		},
		fullEvent: false,
		startAtEnd: true
	}, opts || {});


	let position = opts.position || {};
	if (typeof position === "string") {
		let [binlogName, binlogNextPos] = position.split("::");
		position = {
			binlogName,
			binlogNextPos
		}
	}

	let startParams = {
		binlogName: position.binlogName,
		binlogNextPos: parseInt(position.binlogNextPos || 0),
		startAtEnd: opts.startAtEnd,
		serverId: opts.server_id,
		includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'],
		includeSchema: opts.includeSchema,
		excludeSchema: opts.excludeSchema,
		source: opts.source
	}
	// Require a Starting location
	if (!startParams.binlogName) {
		throw new Error("opts.binlogName and opts.binlogNextPos are required");
	}

	let z = new ZongJi(config);
	let stream = z.start(startParams);

	let timeoutHandle;
	if (opts.stopTime) {
		timeoutHandle = setTimeout(() => stream.end(), Math.min(2147483647, moment(opts.stopTime) - moment()));
	} else if (opts.duration) {
		timeoutHandle = setTimeout(() => stream.end(), Math.min(2147483647, moment.duration(opts.duration).asMilliseconds()));
	}
	if (timeoutHandle) {
		stream.on("finish", () => clearTimeout(timeoutHandle));
	}

	if (opts.fullEvent) {
		return stream;
	} else {
		let groupP = null;
		let units = 0;

		return ls.pipeline(stream, ls.batch({
			count: 1000,
			size: 1024 * 1024 * 3,
			time: {
				milliseconds: 400
			},
			recordCount: p => p.rows ? p.rows.length : 0
		}), ls.through((obj, done) => {
			if (!obj.payload || !obj.payload.length) {
				return done();
			}

			let event = {
				timestamp: obj.timestamp,
				event_source_timestamp: obj.event_source_timestamp,
				correlation_id: obj.correlation_id,
				payload: {}
			};
			let payload = event.payload;
			event.correlation_id.units = 0;
			obj.payload.map(o => {
				let p = o.payload;
				if (!(p.type in payload)) {
					payload[p.type] = {};
				}
				if (!(p.schema in payload[p.type])) {
					payload[p.type][p.schema] = {};
				}
				if (!(p.tableName in payload[p.type][p.schema])) {
					payload[p.type][p.schema][p.tableName] = [];
				}
				event.correlation_id.units += p.rows.length;

				let ids = payload[p.type][p.schema][p.tableName];

				if (p.primaryKey.length == 1) {
					p.rows.forEach(r => {
						if ('after' in r) {
							r = r.after;
						}
						ids.push(r[p.primaryKey[0]]);
					});
				} else {
					p.rows.forEach(r => {
						if ('after' in r) {
							r = r.after;
						}
						ids.push(p.primaryKey.reduce((arr, key) => {
							arr[key] = r[key];
							return arr;
						}, {}));
					});
				}
			});
			done(null, event);
		}));
	}
};

'use strict';

const ZongJi = require("zongji");
const ls = require('leo-streams');
const logger = require('leo-logger')('leo-connector-mysql/listener');
const generateBinlog = require('zongji/lib/sequence/binlog');
const util = require('util');
const merge = require('lodash.merge');

ZongJi.prototype._init = function () {
	let self = this;
	let binlogOptions = {
		tableMap: self.tableMap,
	};
	let firstLog = {
		filename: null,
		logNumber: null,
		position: 4
	};

	let asyncMethods = [
		{
			name: '_isChecksumEnabled',
			callback: function (checksumEnabled) {
				self.useChecksum = checksumEnabled;
				binlogOptions.useChecksum = checksumEnabled;
			}
		},
		{
			name: '_findBinlogs',
			callback: function (result) {
				if (result && result.length) {
					if (self.options.startAtEnd) {
						let row = result[result.length - 1];
						binlogOptions.filename = row.Log_name;
						binlogOptions.position = row.File_size;
					} else {
						firstLog.filename = result[0].Log_name;
						let logNumber = firstLog.filename.match(/\.(\d+)$/);
						firstLog.logNumber = logNumber && logNumber[1];
					}
				}
			}
		}
	];

	let methodIndex = 0;
	let nextMethod = function () {
		let method = asyncMethods[methodIndex];
		self[method.name](function (/* args */) {
			method.callback.apply(this, arguments);
			methodIndex++;
			if (methodIndex < asyncMethods.length) {
				nextMethod();
			}
			else {
				ready();
			}
		});
	};
	nextMethod();

	let ready = function () {
		// Run asynchronously from _init(), as serverId option set in start()
		if (self.options.serverId !== undefined) {
			binlogOptions.serverId = self.options.serverId;
		}

		if (self.options.binlogName && self.options.binlogNextPos) {
			binlogOptions.filename = self.options.binlogName;
			binlogOptions.position = self.options.binlogNextPos;

			let selectedLogNumber = binlogOptions.filename.match(/\.(\d+)$/);
			if (selectedLogNumber && selectedLogNumber[1] && selectedLogNumber[1] < firstLog.logNumber) {
				binlogOptions.filename = firstLog.filename;
				binlogOptions.position = firstLog.position;
			}
		}

		self.binlog = generateBinlog.call(self, binlogOptions);
		self.ready = true;
		self._executeCtrlCallbacks();
	};
};

ZongJi.prototype._findBinlogs = function (next) {
	let self = this;
	self.ctrlConnection.query('SHOW BINARY LOGS', function (err, rows) {
		if (err) {
			// Errors should be emitted
			self.emit('error', err);
			return;
		}
		next(rows);
	});
};

let tableInfoQueryTemplate = `SELECT 
	COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, 
	COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY 
	FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s'`;

ZongJi.prototype._fetchTableInfo = function (tableMapEvent, next) {
	let self = this;
	let sql = util.format(tableInfoQueryTemplate,
		tableMapEvent.schemaName, tableMapEvent.tableName);

	this.ctrlConnection.query(sql, function (err, rows) {
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
			primaryKey: rows.filter(r => r.COLUMN_KEY === 'PRI').map(r => r.COLUMN_NAME)
		};

		next();
	});
};

ZongJi.prototype.start = function (options) {
	let self = this;
	self.set(options);

	let pass = ls.passthrough({
		objectMode: true
	});

	let passEnd = pass.end;
	pass.end = function () {
		self.connection.destroy();

		self.ctrlConnection.query(
			'KILL ' + self.connection.threadId,
			function () {
				if (self.ctrlConnectionOwner) {
					self.ctrlConnection.destroy();
				}

				passEnd.call(pass);
			}
		);
	};

	let _start = function () {
		self.connection._implyConnect();
		self.connection._protocol._enqueue(new self.binlog(function (error, event) {
			if (error) return pass.emit('error', error);
			// Do not emit events that have been filtered out
			if (event === undefined || event._filtered === true) return;

			switch (event.getTypeName()) {
				case 'TableMap':
					let tableMap = self.tableMap[event.tableId];

					if (!tableMap) {
						self.connection.pause();
						self._fetchTableInfo(event, function () {
							// merge the column info with metadata
							event.updateColumnInfo();
							self.connection.resume();
						});
						return;
					}
					break;

				case 'Rotate':
					if (self.binlogName !== event.binlogName) {
						self.binlogName = event.binlogName;
					}

					let emitEvent = {
						rotate: {
							position: event.position,
							binlogName: event.binlogName
						}
					};
					pass.write(emitEvent);
					return;
					break;
			}
			self.binlogNextPos = event.nextPosition;

			let type = event.getTypeName().toLowerCase();
			if (type === 'writerows' || type === 'updaterows') {
				type = 'update';
			} else if (type === 'deleterows') {
				type = 'delete';
			}

			let emitEvent = {
				timestamp: event.timestamp,
				nextPosition: event.nextPosition,
				binlogName: event._zongji && event._zongji.binlogName && event._zongji.binlogName,
				binlogNextPos: event._zongji && event._zongji.binlogNextPos && event._zongji.binlogNextPos,
				type: type,
				database: event.tableMap[event.tableId].parentSchema,
				table: event.tableMap[event.tableId].tableName,
				primaryKey: event.tableMap[event.tableId].primaryKey,
				data: event.rows
			};

			let result = pass.write(emitEvent);

			if (!result) {
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

module.exports = function (connection, tables, opts) {
	opts = merge({
		config: {},
		duration: undefined,
		start: '0::0',
		batch: undefined,
		omitTables: undefined,
		source: 'system:mysql'
	}, opts);

	let start = opts.start.split('::');

	// Client code
	// @todo change connection from config to an actual connection
	let listener = new ZongJi(connection);

	let includeSchema = {};
	let excludeSchema = {};
	if (opts.config.database) {
		if (tables && tables.length) {
			// log only selected tables
			includeSchema[opts.config.database] = Object.keys(tables);
		} else {
			// log all table changes for the selected database
			includeSchema[opts.config.database] = true;
		}
	} else {
		// undefined for all databases except the excluded
		includeSchema = undefined;

		// omit default mysql tables
		excludeSchema = {
			information_schema: true,
			mysql: true,
			performance_schema: true,
			sys: true
		};
	}

	let stream = listener.start({
		binlogName: start[0] || undefined,
		binlogNextPos: parseInt(start[1]) > 3 && start[1] || undefined,
		serverId: opts.config.server_id || 1,
		includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'],
		includeSchema: includeSchema,
		excludeSchema: excludeSchema
	});

	// by default, pass in 1000 records or whatever we get in 1 second
	let batch = ls.batch(opts.batch || {
		count: 1000,
		size: 1024 * 1024 * 3,
		time: {
			seconds: 1
		}
	});

	if (opts.duration) {
		// End the stream and listener after 285 seconds (4:45)
		setTimeout(() => {
			stream.end();
		}, opts.duration * 0.8);
	}

	return ls.pipeline(stream
		, ls.through(function (obj, done) {

			if (!obj.data) {
				return done();
			}

			if (!Array.isArray(obj.data)) {
				obj.data = [obj.data];
			}

			obj.data.map(item => {
				// don't process omitted tables
				if (opts.omitTables && opts.omitTables.indexOf(obj.table) !== -1) {
					logger.debug('Omitting record from table', obj.table);
					return;
				}

				let writeEvent = {
					type: obj.type,
					database: obj.database,
					table: obj.table,
					payload: item.after || item,
					binlogName: obj.binlogName,
					binlogNextPos: obj.binlogNextPos,
					primaryKey: obj.primaryKey,
					timestamp: obj.timestamp
				};

				this.push(writeEvent);
			});

			done();
		})
		, batch
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
				let eid = row.binlogName + '::' + row.binlogNextPos;

				// set the correlation data
				// set start only if it doesn't exist (it'll be our first result)
				if (!batch.correlation_id.start) {
					batch.correlation_id.start = eid;
				}

				// always set the end. It'll end up being the last one in the batch.
				batch.correlation_id.end = eid;
				// batch.eid = eid;
				// batch.event_source_timestamp = row.timestamp;
				logger.info('eid', eid);

				// set the payload
				if (!payload[row.type]) {
					payload[type] = {};
				}
				if (!payload[row.type][row.database]) {
					payload[row.type][row.database] = {};
				}
				if (!payload[row.type][row.database][row.table]) {
					payload[row.type][row.database][row.table] = [];
				}

				// tables[table] will be the either a single PK or a Composite Key
				if (tables && tables[row.table]) {
					if (typeof tables[row.table] === 'string') {
						payload[row.type][row.database][row.table].push(row.payload[tables[row.table]]);
					} else {
						// handle composite keys
						let tmp = {};
						for (let id of tables[row.table]) {
							tmp[id] = row.payload[id];
						}

						payload[row.type][row.database][row.table].push(tmp);
					}
				} else if (row.primaryKey && row.primaryKey.length) {
					// only create a payload if we have one or more primary keys
					// primary key is an array
					let tmp = {};

					if (row.primaryKey.length > 1) {
						for (let id of row.primaryKey) {
							tmp[id] = row.payload[id];
						}
					} else {
						tmp = row.payload[row.primaryKey[0]];
					}

					payload[row.type][row.database][row.table].push(tmp);
				}
			});

			batch.payload = payload;

			done(null, batch);
		})
	);
};

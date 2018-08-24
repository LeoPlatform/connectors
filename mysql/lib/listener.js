'use strict';

const mysql = require('mysql');
const Connection = require('mysql/lib/Connection');
const Pool = require('mysql/lib/Pool');

const util = require('util');
const EventEmitter = require('events').EventEmitter;

const generateBinlog = require('zongji/lib/sequence/binlog');

const alternateDsn = [
	{
		type: Connection, config: function (obj) {
			return obj.config;
		}
	},
	{
		type: Pool, config: function (obj) {
			return obj.config.connectionConfig;
		}
	}
];

function ZongJi(dsn, options) {
	this.set(options);

	EventEmitter.call(this);

	var binlogDsn;

	// one connection to send table info query
	// Check first argument against possible connection objects
	for (var i = 0; i < alternateDsn.length; i++) {
		if (dsn instanceof alternateDsn[i].type) {
			this.ctrlConnection = dsn;
			this.ctrlConnectionOwner = false;
			binlogDsn = cloneObjectSimple(alternateDsn[i].config(dsn));
		}
	}

	if (!binlogDsn) {
		// assuming that the object passed is the connection settings
		var ctrlDsn = cloneObjectSimple(dsn);
		this.ctrlConnection = mysql.createConnection(ctrlDsn);
		this.ctrlConnection.on('error', this._emitError.bind(this));
		this.ctrlConnection.on('unhandledError', this._emitError.bind(this));
		this.ctrlConnection.connect();
		this.ctrlConnectionOwner = true;

		binlogDsn = dsn;
	}
	this.ctrlCallbacks = [];

	this.connection = mysql.createConnection(binlogDsn);
	this.connection.on('error', this._emitError.bind(this));
	this.connection.on('unhandledError', this._emitError.bind(this));

	this.tableMap = {};
	this.ready = false;
	this.useChecksum = false;
	// Include 'rotate' events to keep these properties updated
	this.binlogName = null;
	this.binlogNextPos = null;

	this._init();
}

var cloneObjectSimple = function (obj) {
	var out = {};
	for (var i in obj) {
		if (obj.hasOwnProperty(i)) {
			out[i] = obj[i];
		}
	}
	return out;
};

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._init = function () {
	var self = this;
	var binlogOptions = {
		tableMap: self.tableMap,
	};
	let firstLog = {
		filename: null,
		logNumber: null,
		position: 4
	};

	var asyncMethods = [
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

	var methodIndex = 0;
	var nextMethod = function () {
		var method = asyncMethods[methodIndex];
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

	var ready = function () {
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

ZongJi.prototype._isChecksumEnabled = function (next) {
	var self = this;
	var sql = 'select @@GLOBAL.binlog_checksum as checksum';
	var ctrlConnection = self.ctrlConnection;
	var connection = self.connection;

	ctrlConnection.query(sql, function (err, rows) {
		if (err) {
			if (err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
				// MySQL < 5.6.2 does not support @@GLOBAL.binlog_checksum
				return next(false);
			} else {
				// Any other errors should be emitted
				self.emit('error', err);
				return;
			}
		}

		var checksumEnabled = true;
		if (rows[0].checksum === 'NONE') {
			checksumEnabled = false;
		}

		var setChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';
		if (checksumEnabled) {
			connection.query(setChecksumSql, function (err) {
				if (err) {
					// Errors should be emitted
					self.emit('error', err);
					return;
				}
				next(checksumEnabled);
			});
		} else {
			next(checksumEnabled);
		}
	});
};

ZongJi.prototype._findBinlogs = function (next) {
	var self = this;
	self.ctrlConnection.query('SHOW BINARY LOGS', function (err, rows) {
		if (err) {
			// Errors should be emitted
			self.emit('error', err);
			return;
		}
		next(rows);
	});
};

ZongJi.prototype._executeCtrlCallbacks = function () {
	if (this.ctrlCallbacks.length > 0) {
		this.ctrlCallbacks.forEach(function (cb) {
			setImmediate(cb);
		});
	}
};

var tableInfoQueryTemplate = 'SELECT ' +
	'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
	'COLUMN_COMMENT, COLUMN_TYPE ' +
	'FROM information_schema.columns ' + "WHERE table_schema='%s' AND table_name='%s'";

ZongJi.prototype._fetchTableInfo = function (tableMapEvent, next) {
	var self = this;
	var sql = util.format(tableInfoQueryTemplate,
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
			tableName: tableMapEvent.tableName
		};

		next();
	});
};

ZongJi.prototype.set = function (options) {
	this.options = options || {};
};

ZongJi.prototype.start = function (options) {
	var self = this;
	self.set(options);

	var _start = function () {
		self.connection._implyConnect();
		self.connection._protocol._enqueue(new self.binlog(function (error, event) {
			if (error) return self.emit('error', error);
			// Do not emit events that have been filtered out
			if (event === undefined || event._filtered === true) return;

			switch (event.getTypeName()) {
				case 'TableMap':
					var tableMap = self.tableMap[event.tableId];

					if (!tableMap) {
						self.connection.pause();
						self._fetchTableInfo(event, function () {
							// merge the column info with metadata
							event.updateColumnInfo();
							let emitEvent = {
								tableMap: {
									timestamp: event.timestamp,
									nextPosition: event.nextPosition,
									tableName: event.tableName,
								}
							};

							self.emit('binlog', emitEvent);
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
					self.emit('binlog', emitEvent);
					return;
					break;
			}
			self.binlogNextPos = event.nextPosition;

			let eventName = event.getTypeName().toLowerCase();
			let tableName = event.tableMap[event.tableId].tableName;
			let emitEvent = {};

			emitEvent[eventName] = {
				timestamp: event.timestamp,
				nextPosition: event.nextPosition,
				// tableMap: event.tableMap,
				binlogName: event._zongji && event._zongji.binlogName && event._zongji.binlogName,
				binlogNextPos: event._zongji && event._zongji.binlogNextPos && event._zongji.binlogNextPos,
				tableName: tableName,
				data: event.rows && event.rows[0] && event.rows[0]
			};

			// everything else
			self.emit('binlog', emitEvent);
		}));
	};

	if (this.ready) {
		_start();
	}
	else {
		this.ctrlCallbacks.push(_start);
	}
};

ZongJi.prototype.stop = function () {
	var self = this;
	// Binary log connection does not end with destroy()
	self.connection.destroy();
	self.ctrlConnection.query(
		'KILL ' + self.connection.threadId,
		function () {
			if (self.ctrlConnectionOwner)
				self.ctrlConnection.destroy();
		}
	);
};

ZongJi.prototype._skipEvent = function (eventName) {
	var include = this.options.includeEvents;
	var exclude = this.options.excludeEvents;
	return !(
		(include === undefined ||
			(include instanceof Array && include.indexOf(eventName) !== -1)) &&
		(exclude === undefined ||
			(exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

ZongJi.prototype._skipSchema = function (database, table) {
	var include = this.options.includeSchema;
	var exclude = this.options.excludeSchema;
	return !(
		(include === undefined ||
			(database !== undefined && (database in include) &&
				(include[database] === true ||
					(include[database] instanceof Array &&
						include[database].indexOf(table) !== -1)))) &&
		(exclude === undefined ||
			(database !== undefined &&
				(!(database in exclude) ||
					(exclude[database] !== true &&
						(exclude[database] instanceof Array &&
							exclude[database].indexOf(table) === -1))))));
};

ZongJi.prototype._emitError = function (error) {
	this.emit('error', error);
};

module.exports = ZongJi;

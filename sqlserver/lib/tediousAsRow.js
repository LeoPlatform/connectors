const base = require("mssql/lib/base.js");
const tds = require("tedious");

const debug = require('debug')('mssql:tedious');

const TYPES = require('mssql/lib/datatypes').TYPES
const declare = require('mssql/lib/datatypes').declare
const cast = require('mssql/lib/datatypes').cast
const DECLARATIONS = require('mssql/lib/datatypes').DECLARATIONS
const UDT = require('mssql/lib/udt').PARSERS
const Table = require('mssql/lib/table')

const JSON_COLUMN_ID = 'JSON_F52E2B61-18A1-11d1-B105-00805F49916B'
const XML_COLUMN_ID = 'XML_F52E2B61-18A1-11d1-B105-00805F49916B'

const getTediousType = function(type) {
	switch (type) {
		case TYPES.VarChar:
			return tds.TYPES.VarChar
		case TYPES.NVarChar:
			return tds.TYPES.NVarChar
		case TYPES.Text:
			return tds.TYPES.Text
		case TYPES.Int:
			return tds.TYPES.Int
		case TYPES.BigInt:
			return tds.TYPES.BigInt
		case TYPES.TinyInt:
			return tds.TYPES.TinyInt
		case TYPES.SmallInt:
			return tds.TYPES.SmallInt
		case TYPES.Bit:
			return tds.TYPES.Bit
		case TYPES.Float:
			return tds.TYPES.Float
		case TYPES.Decimal:
			return tds.TYPES.Decimal
		case TYPES.Numeric:
			return tds.TYPES.Numeric
		case TYPES.Real:
			return tds.TYPES.Real
		case TYPES.Money:
			return tds.TYPES.Money
		case TYPES.SmallMoney:
			return tds.TYPES.SmallMoney
		case TYPES.Time:
			return tds.TYPES.TimeN
		case TYPES.Date:
			return tds.TYPES.DateN
		case TYPES.DateTime:
			return tds.TYPES.DateTime
		case TYPES.DateTime2:
			return tds.TYPES.DateTime2N
		case TYPES.DateTimeOffset:
			return tds.TYPES.DateTimeOffsetN
		case TYPES.SmallDateTime:
			return tds.TYPES.SmallDateTime
		case TYPES.UniqueIdentifier:
			return tds.TYPES.UniqueIdentifierN
		case TYPES.Xml:
			return tds.TYPES.NVarChar
		case TYPES.Char:
			return tds.TYPES.Char
		case TYPES.NChar:
			return tds.TYPES.NChar
		case TYPES.NText:
			return tds.TYPES.NVarChar
		case TYPES.Image:
			return tds.TYPES.Image
		case TYPES.Binary:
			return tds.TYPES.Binary
		case TYPES.VarBinary:
			return tds.TYPES.VarBinary
		case TYPES.UDT:
		case TYPES.Geography:
		case TYPES.Geometry:
			return tds.TYPES.UDT
		case TYPES.TVP:
			return tds.TYPES.TVP
		case TYPES.Variant:
			return tds.TYPES.Variant
		default:
			return type
	}
}

const RequestError = base.RequestError;
const ConnectionError = base.ConnectionError;


const valueCorrection = function(value, metadata) {
	if ((metadata.type === tds.TYPES.UDT) && (value != null)) {
		if (UDT[metadata.udtInfo.typeName]) {
			return UDT[metadata.udtInfo.typeName](value)
		} else {
			return value
		}
	} else {
		return value
	}
}



base.Request.prototype.queryRow = function(command, callback) {
	if (!this.parent) {
		return setImmediate(callback, new RequestError('No connection is specified for that request.', 'ENOCONN'))
	}

	if (!this.parent.connected) {
		return setImmediate(callback, new ConnectionError('Connection is closed.', 'ECONNCLOSED'))
	}
	this.canceled = false;
	debug('req: query:', command)

	const recordsets = []
	const errors = []
	const errorHandlers = {}
	const output = {}
	const rowsAffected = []

	let columns = {}
	let recordset = []
	let batchLastRow = null
	let batchHasOutput = false
	let isChunkedRecordset = false
	let chunksBuffer = null
	let hasReturned = false

	const handleError = (doReturn, connection, info) => {
		let err = new Error(info.message)
		err.info = info
		err = new base.RequestError(err, 'EREQUEST')

		if (this.stream) {
			this.emit('error', err)
		} else {
			if (doReturn && !hasReturned) {
				if (connection) {
					for (let event in errorHandlers) {
						connection.removeListener(event, errorHandlers[event])
					}

					this.parent.release(connection)
				}

				hasReturned = true
				callback(err)
			}
		}

		// we must collect errors even in stream mode
		errors.push(err)
	}

	let returnColumns = [];
	const handleInfo = msg => {
		this.emit('info', {
			message: msg.message,
			number: msg.number,
			state: msg.state,
			class: msg.class,
			lineNumber: msg.lineNumber,
			serverName: msg.serverName,
			procName: msg.procName
		})
	}

	this.parent.acquire(this, (err, connection, config) => {
		if (err) return callback(err)

		let row

		if (this.canceled) {
			debug('req: canceling')
			this.parent.release(connection)
			return callback(new base.RequestError('Canceled.', 'ECANCEL'))
		}

		this._cancel = () => {
			debug('req: cancel')
			connection.cancel()
		}

		// attach handler to handle multiple error messages
		connection.on('infoMessage', errorHandlers.infoMessage = handleInfo)
		connection.on('errorMessage', errorHandlers.errorMessage = handleError.bind(null, false, connection))
		connection.on('error', errorHandlers.error = handleError.bind(null, true, connection))

		const req = new tds.Request(command, err => {
			// to make sure we handle no-sql errors as well
			if (err && (!errors.length || (errors.length && err.message !== errors[errors.length - 1].message))) {
				err = new base.RequestError(err, 'EREQUEST')
				if (this.stream) this.emit('error', err)
				errors.push(err)
			}

			// process batch outputs
			if (batchHasOutput) {
				if (!this.stream) batchLastRow = recordsets.pop()[0]

				for (let name in batchLastRow) {
					let value = batchLastRow[name]
					if (name !== '___return___') {
						output[name] = value === tds.TYPES.Null ? null : value
					}
				}
			}

			this._cancel = null

			let error
			if (errors.length && !this.stream) {
				error = errors.pop()
				error.precedingErrors = errors
			}

			if (!hasReturned) {
				for (let event in errorHandlers) {
					connection.removeListener(event, errorHandlers[event])
				}

				this.parent.release(connection)
				hasReturned = true

				if (error) {
					debug('req: query fail', error)
				} else {
					debug('req: query ok')
				}
				if (this.stream) {
					callback(null, {
						recordsets: null,
						recordset: null,
						columns: returnColumns,
						output,
						rowsAffected
					});
				} else {
					callback(null, {
						recordsets,
						recordset: recordsets && recordsets[0],
						columns: returnColumns,
						output,
						rowsAffected
					});
				}
			}
		});

		req.on('columnMetadata', metadata => {
			returnColumns = columns = metadata.map(c => {
				return {
					name: c.colName,
					type: c.type
				};
			});

			isChunkedRecordset = false
			if (metadata.length === 1 && (metadata[0].colName === JSON_COLUMN_ID || metadata[0].colName === XML_COLUMN_ID)) {
				isChunkedRecordset = true
				chunksBuffer = []
			}

			if (this.stream) {
				if (this._isBatch) {
					// don't stream recordset with output values in batches
					if (!columns.___return___) {
						this.emit('recordset', columns)
					}
				} else {
					this.emit('recordset', columns)
				}
			}
		})

		const doneHandler = (rowCount, more) => {
			if (rowCount != null) rowsAffected.push(rowCount)
			// this function is called even when select only set variables so we should skip adding a new recordset
			if (Object.keys(columns).length === 0) return

			if (isChunkedRecordset) {
				if (columns[JSON_COLUMN_ID] && config.parseJSON === true) {
					try {
						row = JSON.parse(chunksBuffer.join(''))
					} catch (ex) {
						row = null
						const ex2 = new base.RequestError(new Error(`Failed to parse incoming JSON. ${ex.message}`), 'EJSON')

						if (this.stream) this.emit('error', ex2)

						// we must collect errors even in stream mode
						errors.push(ex2)
					}
				} else {
					row = {}
					row[Object.keys(columns)[0]] = chunksBuffer.join('')
				}

				chunksBuffer = null

				if (this.stream) {
					this.emit('row', row)
				} else {
					recordset.push(row)
				}
			}

			if (!this.stream) {
				// all rows of current recordset loaded
				Object.defineProperty(recordset, 'columns', {
					enumerable: false,
					configurable: true,
					value: columns
				})

				Object.defineProperty(recordset, 'toTable', {
					enumerable: false,
					configurable: true,
					value() {
						return Table.fromRecordset(this)
					}
				})

				recordsets.push(recordset)
			}

			recordset = []
			columns = {}
		}

		req.on('doneInProc', doneHandler) // doneInProc handlers are used in both queries and batches
		req.on('done', doneHandler) // done handlers are used in batches

		req.on('returnValue', (parameterName, value, metadata) => {
			output[parameterName] = value === tds.TYPES.Null ? null : value
		})

		req.on('row', columns => {
			if (!recordset) recordset = []

			if (isChunkedRecordset) {
				return chunksBuffer.push(columns[0].value)
			}

			row = []
			for (let col of columns) {
				col.value = valueCorrection(col.value, col.metadata)
				row.push(col.value);
			}

			if (this.stream) {
				if (this._isBatch) {
					// dont stream recordset with output values in batches
					if (row.___return___) {
						batchLastRow = row
					} else {
						this.emit('row', row)
					}
				} else {
					this.emit('row', row)
				}
			} else {
				recordset.push(row)
			}
		})

		if (this._isBatch) {
			if (Object.keys(this.parameters).length) {
				for (let name in this.parameters) {
					let param = this.parameters[name]
					let value = getTediousType(param.type).validate(param.value)

					if (value instanceof TypeError) {
						value = new base.RequestError(`Validation failed for parameter '${name}'. ${value.message}`, 'EPARAM')

						this.parent.release(connection)
						return callback(value)
					}

					param.value = value
				}

				const declarations = []
				for (let name in this.parameters) {
					let param = this.parameters[name]
					declarations.push(`@${name} ${declare(param.type, param)}`)
				}

				const assigns = []
				for (let name in this.parameters) {
					let param = this.parameters[name]
					assigns.push(`@${name} = ${cast(param.value, param.type, param)}`)
				}

				const selects = []
				for (let name in this.parameters) {
					let param = this.parameters[name]
					if (param.io === 2) {
						selects.push(`@${name} as [${name}]`)
					}
				}

				batchHasOutput = selects.length > 0

				req.sqlTextOrProcedure = `declare ${declarations.join(', ')};select ${assigns.join(', ')};${req.sqlTextOrProcedure};${batchHasOutput ? (`select 1 as [___return___], ${selects.join(', ')}`) : ''}`
			}
		} else {
			for (let name in this.parameters) {
				let param = this.parameters[name]
				if (param.io === 1) {
					req.addParameter(param.name, getTediousType(param.type), parameterCorrection(param.value), {
						length: param.length,
						scale: param.scale,
						precision: param.precision
					})
				} else {
					req.addOutputParameter(param.name, getTediousType(param.type), parameterCorrection(param.value), {
						length: param.length,
						scale: param.scale,
						precision: param.precision
					})
				}
			}
		}

		connection[this._isBatch ? 'execSqlBatch' : 'execSql'](req);
	});
};

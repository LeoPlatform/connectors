'use strict';

const mssql = require("mssql");
const logger = require('leo-logger')('connector.sql.mssql');

module.exports = async function(config) {
	// make the config parameters the same as the other database types.
	if (config.host && typeof config.server === 'undefined') {
		config.server = config.host;
	}
	config = Object.assign({
		user: 'root',
		password: 'test',
		server: 'localhost',
		database: 'sourcedata',
		port: 1433,
		requestTimeout: 1000 * 50,
		pool: {
			max: 1
		}
	}, config);

    console.log('CREATING NEW SQLSERVER CONNECTION');
    let pool = new mssql.ConnectionPool(config);
    await pool.connect();

	pool.on('error', err => {
		logger.error('Connect pool error', err);
	});
	console.log('SQL Server Pool connected');

	let queryCount = 0;
	return {
		query: async function(query, params, opts = {}) {
			opts = Object.assign({
				inRowMode: true,
				stream: false
			}, opts || {});

            let queryId = ++queryCount;
            let request = pool.request();

            if (params) {
                for (let i in params) {
                    request.input(i, params[i]);
                }
            }
            if (opts.stream === true) {
                request.stream = true;
            }

            logger.debug(`Running query #${queryId}`, query);

            return request.query(query);
		},
        range: async function(table, id) {
		    let request = pool.request();
            let result = await request.query(`select min(${id}) as min, max(${id}) as max, count(${id}) as total from ${table}`);

            return {
                min: result.recordset[0].min,
                max: result.recordset[0].max,
                total: result.recordset[0].total
            };
        },
        nibble: async function(table, id, start, min, max, limit, reverse) {
            let request = pool.request();
            let sql;
            if (reverse) {
                sql = `select ${id} as id from ${table}  
							where ${id} <= ${start} and ${id} >= ${min}
							ORDER BY ${id} desc
							OFFSET ${limit-1} ROWS 
							FETCH NEXT 2 ROWS ONLY`;
            } else {
                sql = `select ${id} as id from ${table}  
							where ${id} >= ${start} and ${id} <= ${max}
							ORDER BY ${id} asc
							OFFSET ${limit-1} ROWS 
							FETCH NEXT 2 ROWS ONLY`;
            }

            let result = await request.query(sql);

            return result.recordset;
        },
        getIds: async function(table, id, start, end, reverse) {
            let request = pool.request();
            let sql;
            if (reverse) {
                sql = `select ${id} as id from ${table}  
					where ${id} <= ${start} and ${id} >= ${end}
					ORDER BY ${id} desc`;
            } else {
                sql = `select ${id} as id from ${table}  
					where ${id} >= ${start} and ${id} <= ${end}
					ORDER BY ${id} asc`;
            }

            let result = await request.query(sql);

            return result.recordset;
        },
		end: function(callback) {
			let err;

			try {
				pool.close();
			} catch (e) {
				err = e;
			}

			if (callback) {
				callback(err);
			} else if (err) {
				throw err;
			}
		}
	};
};

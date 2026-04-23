'use strict'

const connector = require('../../')

const squel = require('squel').useFlavour('mssql')
const bluebird = require('bluebird')

module.exports = async (params) => {
  const connectionInfo = {
    user: params.user,
    password: params.password,
    server: params.host,
    database: params.database,
    port: parseInt(params.port || 1433),
    requestTimeout: 1000 * 50,
    connectionLimit: 1,
    options: { encrypt: false }
  }
  console.log('connector', connectionInfo)
  const pool = connector.connect(connectionInfo)

  pool.insert = async (table, obj) => {
    const sql = squel.insert().into(table)
    Object.keys(obj).forEach(s => sql.set(s, obj[s]))
    await pool.execute(sql.toString())
  }

  pool.insertAll = async (arr) => {
    await bluebird.map(arr, q => {
      const key = Object.keys(q)[0]
      return pool.insert(key, q[key])
    }, { concurrency: 5 })
  }

  pool.executeAll = async (arr) => {
    await bluebird.map(arr, q => {
      return pool.execute(q)
    }, { concurrency: 5 })
  }

  pool.execute = async (query) => {
    return new Promise((resolve, reject) => {
      pool.query(query, (err, result) => {
        if (err) reject(err)
        resolve(result)
      })
    })
  }

  pool.truncate = async (table) => {
    await pool.execute(`DELETE FROM ${table}`)
  }

  return pool
}

const mysql = require("mysql");
const logger = require("leo-sdk/logger")("connector.LeoDW.mysql");
const async = require("async");

module.exports = function(ID, config) {
	var m = mysql.createPool(Object.assign({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "test",
		password: "",
		connectionLimit: 10
	}, config || {}));

	return {
		loadCSV(location, callback) {

		}

	};
};
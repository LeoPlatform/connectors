const async = require("async");

const MAX = 5000;
module.exports = {
	processIds: function(sqlClient, obj, sql, idKeys, callback) {
		function processIdList(idlist) {
			let ids = [];
			let tasks = [];
			idlist.forEach(idthing => {
				// array of ids
				if (Array.isArray(idthing)) {
					ids = ids.concat(idthing);
				} else if (idthing && typeof idthing === "object") {
					// idthing is an object with ids
					if (idthing.ids && idthing.ids.length >= 1) {
						tasks.push((done) => {
							async.doWhilst((done) => {
								let lookupIds = idthing.ids.splice(0, MAX);
								if (Array.isArray(idKeys)) {
									lookupIds = "(values " + lookupIds.map(r => {
										return "(" + r.join(',') + ")";
									}) + ")";
									let sql = idthing.query.replace(/\(\?\)/, lookupIds);
									sqlClient.query(sql, (err, results, fields) => {
										if (!err) {
											ids = ids.concat(results.map(row => {
												return idKeys.map(c => {
													return row[c];
												});
											}));
										}
										done(err);
									}, {inRowMode: false});
								} else {
									let sql = idthing.query.replace(/\(\?\)/, lookupIds);
									sqlClient.query(sql, (err, results, fields) => {
										if (!err) {
											let firstColumn = fields[0].name;
											ids = ids.concat(results.map(row => row[firstColumn]));
										}
										done(err);
									}, {inRowMode: false});
								}

							}, () => idthing.ids.length >= MAX, (err) => {
								if (err) {
									done(err);
								} else {
									done();
								}
							});
						});
					}
				} else if (typeof idthing === "string") {
					// idthing is a query string
					tasks.push((done) => {
						sqlClient.query(idthing, (err, results, fields) => {
							if (!err && results.length) {
								let firstColumn = fields[0].name;
								ids = ids.concat(results.map(row => row[firstColumn]));
							}
							done(err);
						}, {inRowMode: false});
					});
				}
			});
			async.parallelLimit(tasks, 10, (err, results) => {
				callback(err, ids);
			});
		}
		let handledReturn = false;
		let result = sql.call({
			hasIds: (a) => {
				if (a && Array.isArray(a) && a.length) {
					return true;
				} else {
					return false;
				}
			}
		}, obj.payload, (err, idlist) => {
			if (!handledReturn) {
				if (err) return callback(err);
				processIdList(idlist);
			}
		});
		if (result && Array.isArray(result)) {
			handledReturn = true;
			processIdList(result);
		}
	}
};

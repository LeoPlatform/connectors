"use strict";
const basicConnector = require('leo-connector-common/checksum/lib/basicConnector');
const logger = require('leo-logger')('leo-mongo-connector.checksum');
const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;
const ObjectID = mongodb.ObjectID;
const ls = require('leo-streams');

module.exports = () => {
	let cacheDB;
	let getId = function (settings, obj, type) {
		if (Array.isArray(obj)) {
			let i = 0;
			if (type === "max") {
				i = obj.length - 1;
			}
			obj = obj[i];
		}
		return obj[settings.id_column];
	};

	let getQueryIdFunction = function (settings) {
		if (settings.extractId) {
			return eval(`(${settings.extractId})`);
		} else if (settings.id_column === "_id") {
			return (v) => new ObjectID(v);
		} else {
			return (v) => v;
		}
	};

	let buildExtractFunction = (settings) => {
		let mapper = (typeof settings.fields === "string") ? settings.fields : settings.map;
		if (mapper) {
			return eval(`(${mapper})`);
		}
		return settings.fields.map(f => obj[f]);
	};

	let getObjects = function (start, end, label, callback) {
		console.log("Calling", label, start, end);
		let settings = this.settings;
		let id = getQueryIdFunction(settings);
		getCollection(settings).then(collection => {

			let extract = buildExtractFunction(settings);
			let where = Object.assign({}, settings.where, {
				[settings.id_column]: {
					$gte: id(start),
					$lte: id(end)
				}
			});

			let method = settings.method || "find";
			let cursor = ls.pipe(collection[method](where, settings.projectionFields || {}, {
				'sort': [
					[settings.id_column, 1]
				]
			}).stream(), ls.through(function (obj, done) {
				this.invalid = () => {
				};
				let result = extract.call(this, obj);
				done(null, result != undefined ? result : undefined);
			}));


			callback(null, cursor);

		}).catch(callback);
	};

	let self = basicConnector({
		wrap: (handler, base) => {
			return function (event, callback) {
				base(event, handler, (err, data) => {
					if (cacheDB) {
						console.log("Closing Database")
						cacheDB.close();
						cacheDB = null;
					}
					callback(err, data);
				});
			}
		},
		batch: function (start, end, callback) {
			getObjects.call(this, start, end, "Batch", callback);
		},
		individual: function (start, end, callback) {
			getObjects.call(this, start, end, "Individual", callback);
		},
		sample: function (ids, callback) {
			console.log("Calling Sample", ids);
			let settings = this.settings;
			let getid = getId.bind(null, settings);
			let idFn = getQueryIdFunction(settings);
			getCollection(settings).then(collection => {
				let extract = buildExtractFunction(settings);

				let where = Object.assign({}, settings.where, {
					[settings.id_column]: {
						$in: ids.map(idFn)
					}
				});

				let method = settings.method || "find";
				let cursor = ls.pipe(collection[method](where, settings.projectionFields || {}, {
					'sort': [
						[settings.id_column, 1]
					]
				}).stream(), ls.through(function (obj, done) {
					this.invalid = () => {
					};
					let result = extract.call(this, obj);
					done(null, result != undefined ? result : undefined);
				}), ls.through((o, done) => {
					if (ids.indexOf(getid(o)) > -1) {
						done(null, o)
					} else {
						done();
					}
				}));

				callback(null, cursor);
			}).catch(callback);
		},
		nibble: function (start, end, limit, reverse, callback) {
			console.log("Calling Nibble", start, end, limit, reverse);

			let settings = this.settings;
			let getid = getId.bind(null, settings);
			let id = getQueryIdFunction(settings);
			let extract = buildExtractFunction(settings);
			let method = settings.method || "find";
			getCollection(settings).then(collection => {
				let where = Object.assign({}, settings.where, {
					[settings.id_column]: {
						$gte: id(start),
						$lte: id(end)
					}
				});
				collection[method](where, settings.projectionFields || {}, {
					sort: [
						[settings.id_column, !reverse ? 1 : -1]
					],
					limit: 2,
					skip: limit - 1
				}).toArray((err, rows) => {

					console.log(err, rows)
					if (err) {
						return callback(err);
					}

					let r1;
					let r = rows[1] && extract.call({
						push: (obj) => {
							if (!reverse) {
								r1 = (r1 && (getid(r1) < getid(obj))) ? r1 : obj;
							} else {
								r1 = (r1 && (getid(r1) > getid(obj))) ? r1 : obj;
							}
						},
						invalid: (obj) => {
							if (!reverse) {
								r1 = (r1 && (getid(r1) < getid(obj))) ? r1 : obj;
							} else {
								r1 = (r1 && (getid(r1) > getid(obj))) ? r1 : obj;
							}
						}
					}, rows[1]);
					r1 = r1 || r || rows[1];

					let r0;
					r = rows[0] && extract.call({
						push: (obj) => {
							if (!reverse) {
								r0 = (r0 && (getid(r0) > getid(obj))) ? r0 : obj;
							} else {
								r0 = (r0 && (getid(r0) < getid(obj))) ? r0 : obj;
							}
						},
						invalid: (obj) => {
							if (!reverse) {
								r0 = (r0 && (getid(r0) > getid(obj))) ? r0 : obj;
							} else {
								r0 = (r0 && (getid(r0) < getid(obj))) ? r0 : obj;
							}
						}
					}, rows[0]);
					r0 = r0 || r || rows[0];

					callback(null, {
						next: r1 ? getid(r1) : null,
						current: r0 ? getid(r0) : null
					});
				});
			}).catch(callback);
		},
		range: async function (start, end, callback) {
			logger.log("Calling Range", start, end);
			let settings = this.settings;
			getCollection(settings).then(collection => {

				let min = Object.assign({}, settings.where);
				let max = Object.assign({}, settings.where);
				let total = Object.assign({}, settings.where);

				let getid = getId.bind(null, settings);
				let id = getQueryIdFunction(settings);
				let extract = buildExtractFunction(settings);

				if (start || end) {
					total[settings.id_column] = {}
				}
				if (start) {
					min[settings.id_column] = {
						$gte: id(start)
					};
					total[settings.id_column].$gte = id(start);
				}
				if (end) {
					max[settings.id_column] = {
						$lte: id(end)
					};
					total[settings.id_column].$lte = id(end);
				}
				let method = settings.method || "findOne";

				collection[method](min, settings.projectionFields || {}, {
					'sort': [
						[settings.id_column, 1]
					],
					"limit": 1
				}, (err, start) => {
					if (err) {
						return callback(err);
					}
					collection[method](max, settings.projectionFields || {}, {
						'sort': [
							[settings.id_column, -1]
						],
						"limit": 1
					}, (err, end) => {
						if (err) {
							return callback(err);
						}
						collection.count(total, (err, total) => {
							if (err) {
								return callback(err);
							}
							let s;
							let r = extract.call({
								push: (obj) => {
									s = (s && (getid(s) < getid(obj))) ? s : obj;
								},
								invalid: (obj) => {
									s = (s && (getid(s) < getid(obj))) ? s : obj;
								}
							}, start);
							s = s || r || start;

							let e;
							r = extract.call({
								push: (obj) => {
									e = (e && (getid(e) > getid(obj))) ? e : obj;
								},
								invalid: (obj) => {
									e = (e && (getid(e) > getid(obj))) ? e : obj;
								}
							}, end);
							e = e || r || end;
							callback(null, {
								min: getid(s) || 0,
								max: getid(e),
								total: total
							});
						});
					});
				});
			}).catch(callback);
		},
		initialize: (data) => {
			console.log("Calling Initialize", data)
			return Promise.resolve({});
		},
		destroy: function (data, callback) {
			console.log("Calling Destroy", data);
			callback();
		},
		delete: function (ids, callback) {
			console.log("Calling Delete", ids);
			let settings = this.settings;
			let idFn = getQueryIdFunction(settings);
			getCollection(settings).then(collection => {
				let where = Object.assign({}, settings.where, {
					[settings.id_column]: {
						$in: ids.map(idFn)
					}
				});

				collection.deleteMany(where, (err, obj) => {
					if (err) {
						console.log(err)
					} else {
						console.log(`Deleted ${obj.result.n} Objects`)
					}
					callback()
				});
			}).catch(callback);
		}
	});

	async function getCollection(settings) {
		let opts = Object.assign({}, settings);
		console.log("Connection Info", opts.database, opts.collection)
		return await new Promise((resolve, reject) => {
			MongoClient.connect(opts.database, (err, db) => {
				if (err) {
					reject(err);
				} else {
					cacheDB = db;
					resolve(db.collection(opts.collection));
				}
			})
		});
	}

	return self;
};

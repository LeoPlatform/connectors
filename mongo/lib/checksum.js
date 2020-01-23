"use strict";
const basicConnector = require("leo-connector-common/checksum/lib/basicConnector");
const logger = require("leo-logger")("leo-mongo-connector.checksum");
const { MongoClient, ObjectID } = require("mongodb");
const ls = require("leo-sdk").streams;

module.exports = () => {
    let cachedMongoClient;
    const getId = (settings, obj, type) => {
        if (Array.isArray(obj)) {
            let i = 0;
            if (type == "max") {
                i = obj.length - 1;
            }
            obj = obj[i];
        }
        return obj[settings.id_column];
    };
    const getQueryIdFunction = (settings) => {
        let fn = (a) => a;
        if (settings.extractId) {
            fn = eval(`(${settings.extractId})`);
        } 
        if (settings.id_column == "_id") {
            return (v) => new ObjectID(fn(v));
        } else {
            return fn;
        }
    };
    const getWhereObject = (settings) => {
        if (settings.where) {
            return {
                "$where": settings.where
            };
        }
    };
    const buildExtractFunction = (settings) => {
        let mapper = (typeof settings.fields == "string") ? settings.fields : settings.map;
        if (mapper) {
            return eval(`(${mapper})`);
        }
        return settings.fields.map(f => obj[f]);
    };
    const getObjects = async function (start, end, label, callback) {
        logger.info("Calling", label, start, end);
        let settings = this.settings;
        let id = getQueryIdFunction(settings);
        let cursor;
        try {
            let collection = await getCollection(settings);
            let extract = buildExtractFunction(settings);
            let whereObject = getWhereObject(settings);
            let where = Object.assign({}, whereObject, {
                [settings.id_column]: {
                    $gte: id(start),
                    $lte: id(end)
                }
            });
            let projection = settings.projectionFields || {};
            let sortObject = {};
            sortObject[settings.id_column] = 1;
            cursor = ls.pipe(
                collection.find(where, projection).sort(sortObject).stream(), 
                ls.through(function (obj, done) {
                    this.invalid = () => { };
                    let result = extract.call(this, obj);
                    done(null, result != undefined ? result : undefined);
                })
            );
        } catch (err) {
            logger.error(err);
            callback(err);
        }
        callback(null, cursor);     //do callback outside of try..catch
    };
    let self = basicConnector({
        wrap: (handler, base) => {
            return function (event, callback) {
                base(event, handler, (err, data) => {
                    if (cachedMongoClient) {
                        logger.info("Closing Database");
                        cachedMongoClient.close();
                        cachedMongoClient = null;
                    }
                    callback(err, data);
                });
            };
        },
        batch: function (start, end, callback) {
            getObjects.call(this, start, end, "Batch", callback);
        },
        individual: function (start, end, callback) {
            getObjects.call(this, start, end, "Individual", callback);
        },
        sample: async function (ids, callback) {
            logger.info("Calling Sample", ids);
            let cursor;
            let settings = this.settings;
            let getid = getId.bind(null, settings);
            try {
                let idFn = getQueryIdFunction(settings);
                let collection = await getCollection(settings);
                let extract = buildExtractFunction(settings);
                let whereObject = getWhereObject(settings);
                let where = Object.assign({}, whereObject, {
                    [settings.id_column]: {
                        $in: ids.map(idFn)
                    }
                });
                let projection = settings.projectionFields || {};
                let sortObject = {};
                sortObject[settings.id_column] = 1;
                cursor = ls.pipe(
                    collection.find(where, projection).sort(sortObject).stream(),
                    ls.through(function (obj, done) {
                        this.invalid = () => { };
                        let result = extract.call(this, obj);
                        done(null, result != undefined ? result : undefined);
                    }), ls.through((o, done) => {
                        if (ids.indexOf(getid(o)) > -1) {
                            done(null, o);
                        } else {
                            done();
                        }
                    })
                );
            } catch (err) {
                logger.error(err);
                callback(err);
            }
            callback(null, cursor);     //do callback outside of try..catch
        },
        nibble: async function (start, end, limit, reverse, callback) {
            logger.info("Calling Nibble", start, end, limit, reverse);
            let r0;
            let r1;
            let settings = this.settings;
            let getid = getId.bind(null, settings);
            try {
                let id = getQueryIdFunction(settings);
                let extract = buildExtractFunction(settings);
                let collection = await getCollection(settings);
                let whereObject = getWhereObject(settings);
                let where = Object.assign({}, whereObject, {
                    [settings.id_column]: {
                        $gte: id(start),
                        $lte: id(end)
                    }
                });
                let projection = settings.projectionFields || {};
                let sortObject = {};
                sortObject[settings.id_column] = !reverse ? 1 : -1;
                let rows = await collection.find(where, projection).sort(sortObject).limit(2).skip(limit-1).toArray();
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
            } catch (err) {
                logger.error(err);
                callback(err);
            }
            callback(null, {    //do callback outside of try..catch
                next: r1 ? getid(r1) : null,
                current: r0 ? getid(r0) : null
            }); 
        },
        range: async function (start, end, callback) {
            logger.info("Calling Range", start, end);
            let s;
            let e;
            let totalResult;
            let settings = this.settings;
            let getid = getId.bind(null, settings);
            try {
                let id = getQueryIdFunction(settings);
                let extract = buildExtractFunction(settings);
                let collection = await getCollection(settings);
                let whereObject = getWhereObject(settings);
                let min = Object.assign({}, whereObject);
                let max = Object.assign({}, whereObject);
                let total = Object.assign({}, whereObject);
                if (start || end) {
                    total[settings.id_column] = {};
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
                logger.debug("min", min);
                logger.debug("max", max);
                let projection = settings.projectionFields || {};
                logger.debug("projection", projection);
                let startSortObject = {};
                startSortObject[settings.id_column] = 1;
                logger.debug("startSortObject", startSortObject);
                let startResult = await collection.find(min, projection).sort(startSortObject).limit(1).toArray();
                logger.debug("startResult", startResult);
                let endSortObject = {};
                endSortObject[settings.id_column] = -1;
                logger.debug("endSortObject", endSortObject);
                let endResult = await collection.find(max, projection).sort(endSortObject).limit(1).toArray();
                logger.debug("endResult", endResult);
                totalResult = await collection.find(total).count();
                logger.debug("totalResult", totalResult);
                let r = extract.call({
                    push: (obj) => {
                        s = (s && (getid(s) < getid(obj))) ? s : obj;
                    },
                    invalid: (obj) => {
                        s = (s && (getid(s) < getid(obj))) ? s : obj;
                    }
                }, startResult[0]);
                s = s || r || startResult[0];
                r = extract.call({
                    push: (obj) => {
                        e = (e && (getid(e) > getid(obj))) ? e : obj;
                    },
                    invalid: (obj) => {
                        e = (e && (getid(e) > getid(obj))) ? e : obj;
                    }
                }, endResult[0]);
                e = e || r || endResult[0];
            } catch (err) {
                logger.error(err);
                callback(err);
            }
            callback(null, {       //do callback outside of try..catch
                min: getid(s) || 0,
                max: getid(e),
                total: totalResult
            });
        },
        initialize: (data) => {
            logger.info("Calling Initialize", data);
            return Promise.resolve({});
        },
        destroy: function (data, callback) {
            logger.info("Calling Destroy", data);
            callback();
        },
        delete: async function (ids, callback) {
            logger.info("Calling Delete", ids);
            let settings = this.settings;
            try {
                let idFn = getQueryIdFunction(settings);
                let collection = await getCollection(settings);
                let whereObject = getWhereObject(settings);
                let where = Object.assign({}, whereObject, {
                    [settings.id_column]: {
                        $in: ids.map(idFn)
                    }
                });
                let obj = await collection.deleteMany(where);
                logger.info(`Deleted ${obj.result.n} Objects`);
            } catch (err) {
                logger.error(err);
                callback(err);
            }
            callback();
        }
    });
    async function getCollection(settings) {
        let opts = Object.assign({}, settings);
        logger.info('Connection Info');
        logger.info('database', opts.database);
        logger.info('collection', opts.collection);
        logger.info('connectString', opts.connectString);
        try {
            let mongoClient;
            // if a ca is passed in, use the provided connect string and ca for SSL; otherwise, just use old way
            if (opts.ca) {
                let ca = opts.ca;
                let connectString = opts.connectString;
                let mongoConfig = {
                    useNewUrlParser: true,
                    sslValidate: true,
                    sslCA: ca
                };
                mongoClient = await MongoClient.connect(connectString, mongoConfig);
            } else {
                mongoClient = await MongoClient.connect(opts.database, { useNewUrlParser: true });
            }
            cachedMongoClient = mongoClient;
            let db = mongoClient.db();
            return db.collection(opts.collection);
        } catch (err) {
            logger.error(err);
            return err;
        }
    }
    return self;
};

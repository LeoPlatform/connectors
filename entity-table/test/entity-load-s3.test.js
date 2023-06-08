let eventstream = require("event-stream");
const crypto = require('crypto');
const sinon = require("sinon");

let aws = require("aws-sdk");

let lib = require("../index");
let leo = require("leo-sdk");
let ls = leo.streams;
let moment = require("moment");
const assert = require("assert");
describe('entity-connector', function() {
	const sandbox = sinon.createSandbox();

	afterEach(() => {
		sandbox.restore();
	});
	it('Should write to s3', async function() {
		this.timeout(10000);
		let now = 1680130898818;
		let utc = moment.utc;
		sandbox.stub(moment, "utc").callsFake(() => utc(now));
		let getObjectCalls = [];
		sandbox.stub(leo.aws.s3, "putObject").callsFake((args) => {
			let hash = crypto.createHash('md5').update(args.Body).digest("hex");
			delete args.Body;
			getObjectCalls.push(args);
			return {
				promise: () => Promise.resolve({
					ContentMD5: hash
				})

			};
		});
		let batchWriteCalls = [];
		sandbox.stub(leo.aws.dynamodb.docClient, "batchWrite").callsFake((params, cb) => {
			batchWriteCalls.push(params);
			cb(null, { UnprocessedItems: {} });
		});
		let botId = "fake-bot-id";
		// let large = [];
		// for (let i = 0; i < 1024 * 400 * 3; i++) {
		// 	let r = Math.floor(Math.random() * 100) + 97;
		// 	large.push(String.fromCodePoint(r));
		// }
		// let dataIn = [{
		// 	obj: {
		// 		account_id: 1,
		// 		some: "data",
		// 		large: large.join("")
		// 	}
		// }];
		//require("fs").writeFileSync("./input-events.json", JSON.stringify(dataIn, null, 2));

		let dataIn = require("./input-events.json");


		await new Promise((resolve, reject) => {
			ls.pipe(
				eventstream.readArray(dataIn),
				lib.load("fake-table", "account", (payload, hash) => {
					let accountId = payload.obj.account_id;
					let hashed = hash("account", accountId);
					return Object.assign({}, payload.obj, {
						id: accountId.toString(),
						partition: hashed,
					});
				}, {
					botId: botId,
					merge: false,
					records: 1000,
					system: 'system:ddb-account-entity',
				}),
				(err) => {
					err ? reject(err) : resolve();
				}
			);
		});

		assert.deepEqual(getObjectCalls.map(a => {
			a.Key = a.Key.replace(/(-[a-z0-9]+)+/, "-uuid");
			return a;
		}), [{
			Bucket: undefined,
			Key: "files/entities/account/z/2023/03/29/23/01/1680130898818-uuid.gz"
		}]);
		assert.deepEqual(batchWriteCalls.map(b => {
			b.RequestItems["fake-table"].forEach(a => {
				if (a.PutRequest.Item.s3Reference) {
					a.PutRequest.Item.s3Reference.key = a.PutRequest.Item.s3Reference.key.replace(/(-[a-z0-9]+)+/, "-uuid");
				}
			});
			return b;
		}), [
			{
				"RequestItems": {
					"fake-table": [
						{
							"PutRequest": {
								"Item": {
									"id": "1",
									"partition": "account-1",
									"s3Reference": {
										"bucket": undefined,
										"hash": "a2456455643c6ca4df3ebd0e98e23ae4",
										"key": "files/entities/account/z/2023/03/29/23/01/1680130898818-uuid.gz",
									},
								},
							},
						},
						{
							"PutRequest": {
								"Item": {
									"account_id": 2,
									"id": "2",
									"large": "not really",
									"partition": "account-2",
									"some": "other data"
								}
							}
						}
					],
				},
				"ReturnConsumedCapacity": "TOTAL",
			},
		]);
		console.log("Done");
	});

	it('should pull s3 reference', async function() {
		this.timeout(30000);
		let botId = "mock-bot-id";
		let fakeData = [
			{
				partition: "item-3",
				id: 3,
				stuff: "def"
			},
			{
				partition: "item-3",
				id: 3,
				stuff: "ghi"
			}
		].map(d => JSON.stringify(d));
		let getObjectCalls = [];
		sandbox.stub(leo.aws.s3, "getObject").callsFake((args) => {
			getObjectCalls.push(args);
			let toReturn = fakeData.shift();
			return { promise: () => Promise.resolve(toReturn) };
		});

		let writeCalls = [];
		sandbox.stub(leo, "write").callsFake(() => {
			return leo.streams.through((data, done) => {
				writeCalls.push(data);
				done();
			});
		});

		let toCheckpointCalls = [];
		sandbox.stub(leo.streams, "toCheckpoint").callsFake(() => {
			return leo.streams.write((data, done) => {
				toCheckpointCalls.push(data);
				done();
			});
		});


		const baseTime = Date.now();
		const dataIn = {
			Records: [
				// Standard
				{
					eventID: "1",
					eventSourceARN: ":table/mock-table/stream",
					dynamodb: {
						ApproximateCreationDateTime: baseTime + 1,
						OldImage: {
							partition: "item-1",
							id: 1,
							stuff: "abc"
						},
						NewImage: {
							partition: "item-1",
							id: 1,
							stuff: "xyz"
						}
					}
				},
				// Compressed Data
				{
					eventID: "2",
					eventSourceARN: ":table/mock-table/stream",
					dynamodb: {
						ApproximateCreationDateTime: baseTime + 2,
						OldImage: {
							partition: "item-2",
							id: 2,
							compressedData: lib.deflate(JSON.stringify({
								partition: "item-2",
								id: 2,
								stuff: "abc"
							}))
						},
						NewImage: {
							partition: "item-2",
							id: 2,
							compressedData: lib.deflate(JSON.stringify({
								partition: "item-2",
								id: 2,
								stuff: "xyz"
							}))
						}
					}
				},
				// S3 References Diff Hash
				{
					eventID: "3",
					eventSourceARN: ":table/mock-table/stream",
					dynamodb: {
						ApproximateCreationDateTime: baseTime + 3,
						OldImage: {
							partition: "item-3",
							id: 3,
							s3Reference: {
								"bucket": "mock-bucket",
								"hash": "a2456455643c6ca4df3ebd0e98e23ae4",
								"key": "files/entities/account/z/2023/03/29/23/01/1680130898818-uuid.gz",
							}
						},
						NewImage: {
							partition: "item-3",
							id: 3,
							s3Reference: {
								"bucket": "mock-bucket",
								"hash": "a2456455643c6ca4df3ebd0e98e23ae5",
								"key": "files/entities/account/z/2023/03/29/23/01/1680130898819-uuid.gz",
							}
						}
					}
				},
				// S3 References Same Hash
				{
					eventID: "3",
					eventSourceARN: ":table/mock-table/stream",
					dynamodb: {
						ApproximateCreationDateTime: baseTime + 3,
						OldImage: {
							partition: "item-3",
							id: 3,
							s3Reference: {
								"bucket": "mock-bucket",
								"hash": "a2456455643c6ca4df3ebd0e98e23ae4",
								"key": "files/entities/account/z/2023/03/29/23/01/1680130898828-uuid.gz",
							}
						},
						NewImage: {
							partition: "item-3",
							id: 3,
							s3Reference: {
								"bucket": "mock-bucket",
								"hash": "a2456455643c6ca4df3ebd0e98e23ae4",
								"key": "files/entities/account/z/2023/03/29/23/01/1680130898829-uuid.gz",
							}
						}
					}
				}
			].map(r => {
				if (r.dynamodb.OldImage) {
					r.dynamodb.OldImage = aws.DynamoDB.Converter.marshall(r.dynamodb.OldImage);
				}
				if (r.dynamodb.NewImage) {
					r.dynamodb.NewImage = aws.DynamoDB.Converter.marshall(r.dynamodb.NewImage);
				}
				return r;
			})
		};

		await new Promise((resolve, reject) => {
			lib.tableOldNewProcessor({
				botPrefix: 'ddb-',
				botSuffix: '-entity-load-account-entity-old-new',
				eventSuffix: '-entity-old-new',
				resourcePrefix: 'account',
				system: 'system:ddb-account-entity'
			})(
				dataIn,
				{
					botId
				},
				(err) => err ? reject(err) : resolve()
			);
		});

		assert.deepEqual(getObjectCalls, []);
		assert.deepEqual(writeCalls.map(e => {
			delete e.timestamp;
			return e;
		}), [
			{
				correlation_id: {
					source: 'system:ddb-account-entity',
					start: '1'
				},
				event: 'account-entity-old-new',
				event_source_timestamp: (baseTime + 1) * 1000,
				id: 'ddb-account-entity-load-account-entity-old-new',
				payload: {
					old: {
						id: 1,
						partition: "item-1",
						stuff: "abc"
					},
					new: {
						id: 1,
						partition: "item-1",
						stuff: "xyz"
					}
				},
			},
			{
				correlation_id: {
					source: 'system:ddb-account-entity',
					start: '2'
				},
				event: 'account-entity-old-new',
				event_source_timestamp: (baseTime + 2) * 1000,
				id: 'ddb-account-entity-load-account-entity-old-new',
				payload: {
					old: {
						id: 2,
						partition: "item-2",
						stuff: "abc"
					},
					new: {
						id: 2,
						partition: "item-2",
						stuff: "xyz"
					}
				},
			}
		]);
		assert.deepEqual(toCheckpointCalls, []);


	});
});

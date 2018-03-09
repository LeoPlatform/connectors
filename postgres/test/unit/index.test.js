const sinon = require('sinon');
require("../../lib/connect");
const connectStub = sinon
	.stub( require.cache[ require.resolve('../../lib/connect') ], 'exports')
	.callsFake(() => {});
require("leo-connector-common/sql/loader");
const sqlLoaderStub = sinon
	.stub( require.cache[ require.resolve('leo-connector-common/sql/loader') ], 'exports')
	.callsFake(connectFn => connectFn());
require("pg-logical-replication");
let onDataFn;
let onErrFn;
let stopSpy = sinon.spy();
function logicalRepMock() {
	this.on = (str, fn) => {
		if (str === "data") onDataFn = fn;
		if (str === "error") onErrFn = fn;
		return this;
	};
	this.getChanges = () => {};
	this.stop = stopSpy;
}
const logicalRepliationStub = sinon
	.stub( require.cache[ require.resolve("pg-logical-replication")], 'exports')
	.callsFake(logicalRepMock);
const leoPostgres = require('../../');
const {assert} = require('chai');
const ls = require('leo-sdk').streams;

describe('PostgreSQL', () => {
	describe('leo-connector-postgres', () => {
		it('load creates sqlLoader and configures connector', () => {
			leoPostgres.load();
			assert.isTrue(connectStub.calledOnce, "Connect not called");
			assert.isTrue(sqlLoaderStub.calledOnce, "Sql Loader not called");
		});
		it('binLogReader errs', ()  => {
			leoPostgres.binlogReader();

			onErrFn({ test: "Error" });

			assert.isTrue(stopSpy.calledOnce);
		});
		
		it('binLogReader returns a pipable stream', (done)  => {
			const binLogStream = leoPostgres.binlogReader();

			var lineReader = require('readline').createInterface({
				input: require('fs').createReadStream('test/unit/raw-log-obj.txt')
			});

			lineReader.on('line', function (line) {
				const msg = JSON.parse(line);
				msg.log = Buffer.from(msg.log);
				onDataFn(msg);
			});
			
			ls.pipe(binLogStream, ls.log(), ls.devnull(), (err) => {
				console.log("bin log done");
				if (err) console.log(err);
				assert.isTrue(logicalRepliationStub.calledTwice);
				done();
			});

			lineReader.on('close', () => {
				console.log("DONE");
				binLogStream.end();
			});

		});
	});
});

'use strict';

const { expect } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// Stub the parent class so we don't pull in leo-aws/aws-sdk at test time.
class BaseStub {
	constructor(opts) { Object.assign(this, opts); }
}

const connector = proxyquire('../../index.js', {
	'leo-connector-common/base': BaseStub,
	'./lib/connect.js': sinon.stub(),
	'./lib/checksum.js': function checksum() { throw new Error('checksum not implemented'); },
	'./lib/dol': class DolStub { constructor() {} },
});

describe('index.js', () => {
	it('exports a connector with a connect function', () => {
		expect(connector.connect).to.be.a('function');
	});

	it('checksum throws not-implemented', () => {
		expect(() => connector.checksum()).to.throw(/not implemented/);
	});

	it('domainObjectBuilder returns a Dol instance', () => {
		const dol = connector.domainObjectBuilder({ catalog: 'c', schema: 's' });
		expect(dol).to.be.an('object');
	});
});

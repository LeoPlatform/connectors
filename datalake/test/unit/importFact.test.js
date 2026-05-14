'use strict';

const { expect } = require('chai');
const { PassThrough } = require('stream');
const csv = require('fast-csv');

// Test nonNull/CSV serialization directly — this is the function from connect.js
// duplicated here for unit testing the serialization contract.
function nonNull(v) {
	if (v === '' || v === null || v === undefined) return '\\N';
	if (typeof v === 'string' && v.search(/\r/) !== -1) return v.replace(/\r\n?/g, '\n');
	return v;
}

describe('importFact — nonNull / CSV serialization contract', () => {
	it('serializes null as \\N', () => {
		expect(nonNull(null)).to.equal('\\N');
	});

	it('serializes undefined as \\N', () => {
		expect(nonNull(undefined)).to.equal('\\N');
	});

	it('serializes empty string as \\N', () => {
		expect(nonNull('')).to.equal('\\N');
	});

	it('normalizes \\r\\n to \\n', () => {
		expect(nonNull('foo\r\nbar')).to.equal('foo\nbar');
	});

	it('normalizes \\r to \\n', () => {
		expect(nonNull('foo\rbar')).to.equal('foo\nbar');
	});

	it('passes through bare \\n unchanged', () => {
		expect(nonNull('foo\nbar')).to.equal('foo\nbar');
	});

	it('passes through normal string unchanged', () => {
		expect(nonNull('hello world')).to.equal('hello world');
	});

	it('passes through numbers unchanged', () => {
		expect(nonNull(42)).to.equal(42);
	});

	it('serializes booleans as-is (true/false strings handled by fast-csv)', () => {
		expect(nonNull(true)).to.equal(true);
		expect(nonNull(false)).to.equal(false);
	});
});

describe('importFact — CSV output contract', () => {
	function csvRowsFromObjects(objects, columns) {
		return new Promise((resolve, reject) => {
			const ws = csv.createWriteStream({
				headers: false,
				delimiter: '|',
				transform: (row, done) => done(null, columns.map(f => nonNull(row[f]))),
			});
			const output = new PassThrough();
			let buf = '';
			output.on('data', d => { buf += d.toString(); });
			output.on('end', () => resolve(buf));
			output.on('error', reject);
			ws.pipe(output);
			objects.forEach(o => ws.write(o));
			ws.end(() => {});
		});
	}

	it('pipe-delimits fields', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: 'foo', active: true }],
			['id', 'name', 'active']
		);
		expect(output.trim()).to.equal('1|foo|true');
	});

	it('serializes null as \\N in CSV output', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: null }],
			['id', 'name']
		);
		expect(output.trim()).to.equal('1|\\N');
	});

	it('serializes empty string as \\N', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: '' }],
			['id', 'name']
		);
		expect(output.trim()).to.equal('1|\\N');
	});

	it('quotes a field containing a pipe character', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, name: 'foo|bar' }],
			['id', 'name']
		);
		// fast-csv RFC-4180 quoting wraps the field in double-quotes
		expect(output.trim()).to.equal('1|"foo|bar"');
	});

	it('collapses \\r\\n to \\n in a string field', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, notes: 'line1\r\nline2' }],
			['id', 'notes']
		);
		expect(output).to.include('line1\nline2');
		expect(output).to.not.include('\r');
	});

	it('serializes boolean true as "true" string', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, active: true }],
			['id', 'active']
		);
		expect(output.trim()).to.equal('1|true');
	});

	it('serializes boolean false as "false" string', async () => {
		const output = await csvRowsFromObjects(
			[{ id: 1, active: false }],
			['id', 'active']
		);
		expect(output.trim()).to.equal('1|false');
	});
});

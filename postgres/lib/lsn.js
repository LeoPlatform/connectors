// upper and lower is bigger
function compare(a, b) {
	if (a.upper === b.upper && a.lower === b.lower) return 0;
	if (a.upper === b.upper) {
		if (a.lower > b.lower) return 1;
		if (a.lower < b.lower) return -1;
	}
	if (a.upper > b.upper) return 1;
	if (a.upper < b.upper) return -1;
}

function fromWal(log) {
	const upper = log.chunk.readUInt32BE(1);
	const lower = log.chunk.readUInt32BE(5);
	return {
		upper,
		lower,
		string: upper.toString(16).toUpperCase() + "/" + lower.toString(16).toUpperCase()
	};
}

function fromString(lsnStr) {
	const [upper, lower] = lsnStr.split('/');
	return {
		upper: parseInt(upper, 16),
		lower: parseInt(lower, 16),
		string: upper.toString(16).toUpperCase() + "/" + lower.toString(16).toUpperCase()
	};
}

function increment(lsn) {
	const incremented = Object.assign({}, lsn);
	if (incremented.lower === 4294967295) { // [0xff, 0xff, 0xff, 0xff]
		incremented.upper = incremented.upper + 1;
		incremented.lower = 0;
	}
	else {
		incremented.lower = incremented.lower + 1;
	}
	return incremented;
}

module.exports = {
	compare,
	increment,
	fromWal,
	fromString
};

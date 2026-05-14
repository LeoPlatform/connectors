'use strict';
const farmhash = require('farmhash-modern');

// Computes a FarmFingerprint64 surrogate key from an ordered list of natural-key parts.
// Matches the Redshift FARMFINGERPRINT64() convention: null/undefined → empty string,
// parts joined with '|'. Output is a decimal string.
module.exports = function fingerprint64(parts) {
	const joined = parts.map(v => v == null ? '' : String(v)).join('|');
	return farmhash.fingerprint64(joined).toString();
};

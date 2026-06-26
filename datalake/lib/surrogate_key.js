'use strict';
const farmhash = require('farmhash-modern');

// Computes a FarmFingerprint64 surrogate key from an ordered list of natural-key parts.
// Matches the Redshift FARMFINGERPRINT64() convention: null/undefined → empty string,
// parts joined with '|', output as a signed 64-bit decimal string.
//
// farmhash-modern returns an UNSIGNED BigInt; Redshift FARMFINGERPRINT64 returns BIGINT
// (signed). Values > 2^63-1 must be converted to their signed equivalent so they fit in
// a BIGINT column on both sides.
const SIGNED_MAX = (BigInt(1) << BigInt(63)) - BigInt(1);
const MOD_64 = BigInt(1) << BigInt(64);
function toSigned64(unsigned) {
	return unsigned <= SIGNED_MAX ? unsigned : unsigned - MOD_64;
}

module.exports = function fingerprint64(parts) {
	const joined = parts.map(v => v == null ? '' : String(v)).join('|');
	const unsigned = farmhash.fingerprint64(joined);
	// eslint-disable-next-line valid-typeof
	const asBigInt = typeof unsigned === 'bigint' ? unsigned : BigInt(unsigned);
	return toSigned64(asBigInt).toString();
};

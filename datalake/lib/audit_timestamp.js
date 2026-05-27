'use strict';

// Single source of truth for the audit-timestamp shape used by both
// connect.js setAuditdate and dwconnect.js (setAuditdate + per-row audit
// fallback in importFact). Returns `YYYY-MM-DDTHH:MM:SS` — naked-ISO,
// second-resolution, no millis, no trailing Z.
//
// The shape diverges from the postgres/redshift sibling by exactly one
// character: that connector emits `…SSZ` (drops millis, keeps Z, see
// connectors/postgres/lib/connect.js setAuditdate). Databricks read_files
// PERMISSIVE NTZ inference rejects the Z (CSV value → null), so the
// datalake connector strips it; everything else about the shape — including
// the deliberate drop of milliseconds for second-resolution audit
// timestamps — matches the prior convention.
//
// Callers wrap in single quotes when destined for a SQL literal (e.g.
// client.auditdate); for raw CSV-cell or filename use, the unquoted form is
// what's needed.
function naiveIsoNow() {
	return new Date().toISOString().replace(/\.\d*Z$/, '');
}

module.exports = naiveIsoNow;
module.exports.naiveIsoNow = naiveIsoNow;

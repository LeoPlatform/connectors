const merge = require("lodash/merge");

/**
 * Collapse two records that share the same natural key.
 *
 * Records for one key reach this function in arrival order (combine sorts by
 * natural-key hash then by an arrival counter first), so `data` always arrived
 * after `lastObj`.
 *
 * The last event for a key in the batch wins the active-vs-deleted decision, and
 * data is preserved so neither outcome is a bare/sparse row:
 *
 *   - insert/update then delete -> the delete wins, but the accumulated data is kept,
 *     so the row is created-then-soft-closed rather than written as a bare tombstone.
 *     (Previously the insert's data was discarded and a sparse row written.)
 *   - delete then insert/update -> the later write wins and REACTIVATES the entity; the
 *     delete intent is dropped. Loading data after a delete undeletes the row, per
 *     loading data clears the deleted flag — facts set `_deleted = false`
 *     on load; the merge layer applies it).
 *   - lone / leading delete with no data -> stays a bare tombstone (an ordinary
 *     cross-batch delete of a row that already exists in the target) — unchanged.
 *   - two writes, no delete -> deep-merged — unchanged behavior.
 *
 * @param {object} lastObj the earlier record (accumulator)
 * @param {object} data the later record
 * @returns {object} the collapsed record
 */
function combineRecords(lastObj, data) {
	if (data.__leo_delete__) {
		// Later event is a delete: it wins. Stamp the delete markers onto whatever has
		// been accumulated, preserving any data columns so the closed row is populated
		// rather than a bare tombstone.
		lastObj.__leo_delete__ = data.__leo_delete__;
		lastObj.__leo_delete_id__ = data.__leo_delete_id__;
		// The collapsed record now represents the DELETE, so it must carry the delete's
		// arrival sequence, not the earlier write's — otherwise a consumer comparing this
		// record against another group's record would order it as of the wrong event
		// Only present when combine() was constructed with emitSequence.
		if (data.__leo_seq__ !== undefined) {
			lastObj.__leo_seq__ = data.__leo_seq__;
		}
		return lastObj;
	}
	if (lastObj.__leo_delete__) {
		// Earlier event was a delete, later event is a write: the write wins and
		// reactivates the entity. Drop the delete and keep the later record.
		return data;
	}
	return merge(lastObj, data);
}

module.exports = combineRecords;

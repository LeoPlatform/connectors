'use strict';

/**
 * Build the `payload.data` for one delete marker.
 *
 * `combine()` groups records by the table's natural-key columns, so whatever value
 * this function writes under the natural-key column IS the marker's combine group.
 * Two things follow, and the old code got the second one wrong for any table whose
 * natural key is not literally named `id`:
 *
 *   - A delete keyed by the row's OWN natural key must carry that key's real value,
 *     so the marker lands in the same group as the row's writes and combineRecords
 *     can apply last-event-wins.
 *   - A delete keyed by anything else (a parent FK, a source `mongo_id`) must carry
 *     a value that is distinct per deleted id and cannot collide with a real row —
 *     hence the `_del_` prefix. Distinctness is the part that matters: the old code
 *     always wrote this under a column literally named `id`, so for a table whose
 *     natural key is something else (`f_shipping_label_package`, nk `package_id`)
 *     the natural-key column was left undefined on every marker. Every delete in
 *     the batch therefore hashed to the same combine group and all but one were
 *     silently dropped — data loss, not just misordering (RPL-6780).
 *
 * Extracted from load.js's checkforDelete so it can be unit-tested without leo-sdk,
 * the same reason combine-records.js was split out of combine.js.
 *
 * @param {string} field  the column the delete event keys on
 * @param {*} id          the value being deleted
 * @param {string|null} nk  the table's single natural-key column, or null when it
 *                          is unknown here or composite
 * @returns {object} the marker's payload.data
 */
function deleteMarkerData(field, id, nk) {
	const data = {
		__leo_delete__: field,
		__leo_delete_id__: id,
	};

	if (!nk) {
		// Natural key unknown (table absent from tableConfig) or composite — no column
		// to key on, so preserve the historical shape exactly rather than guess.
		data.id = field === 'id' ? id : `_del_${id}`;
		return data;
	}

	data[nk] = field === nk ? id : `_del_${id}`;
	return data;
}

module.exports = deleteMarkerData;

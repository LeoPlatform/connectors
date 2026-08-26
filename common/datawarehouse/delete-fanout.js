'use strict';

const async = require('async');
const transform = require('./transform.js');

/**
 * Expand a single `type: 'delete'` event into one delete marker per affected row.
 *
 * A delete event names a set of rows by some column — often the row's own natural
 * key, but just as often a parent foreign key ("delete every Shipment Item whose
 * shipment_id is X") or a source-system id (`mongo_id`).
 *
 * Ordering only works when the marker carries the row's OWN natural key. `combine`
 * groups records by natural key and `combineRecords` then applies last-event-wins
 * within a group. A marker keyed by anything else lands in a group of its own,
 * is never compared against that row's same-batch writes, and the outcome is
 * decided by the connector's fixed flush/merge order instead of by event order —
 * which is how a delete gets silently reverted (RPL-6780).
 *
 * So when the delete is keyed by a non-natural-key column, ask the target for the
 * natural keys it currently matches and emit a marker per row. Those markers then
 * flow through the existing, already-correct ordering logic unchanged.
 *
 * Resolution is an optional client capability (`client.resolveDeleteKeys`). A
 * connector that does not implement it — or a table whose natural key we cannot
 * determine — falls back to the historical `_del_<value>` marker, preserving
 * today's behavior rather than failing.
 *
 * Known residual gap: a row created in the SAME batch as the delete does not yet
 * exist when the target is queried, so it is not resolved and stays active. That
 * gap is not introduced here — the previous FK-keyed UPDATE also ran before the
 * staging merge and matched nothing. Closing it needs a reconciliation sweep,
 * tracked separately on RPL-6780.
 *
 * @param {object} obj        the raw `type: 'delete'` event
 * @param {object} tableNks   table identifier -> array of natural-key columns
 * @param {object} client     dw client; may expose resolveDeleteKeys(table, field, nk, ids, cb)
 * @param {function} callback (err, records[])
 */
function deleteFanout(obj, tableNks, client, callback) {
	const data = (obj.payload && obj.payload.data) || {};
	const ids = data.in || [];
	const entities = data.entities || [];
	const records = [];

	function record(entity, field, payloadData) {
		return Object.assign({}, obj, {
			payload: {
				type: entity.type,
				entity: entity.name,
				command: 'delete',
				field: field,
				data: payloadData
			}
		});
	}

	// Carries the row's own natural key, so combine() groups it with that row's
	// writes and combineRecords decides which event actually won.
	function resolved(entity, nk, value) {
		const payloadData = {
			__leo_delete__: nk,
			__leo_delete_id__: value
		};
		payloadData[nk] = value;
		return record(entity, nk, payloadData);
	}

	// Historical marker: keyed by a column that is not the row's natural key, so it
	// is isolated from the row's writes and cannot participate in ordering.
	function unresolved(entity, field, value) {
		return record(entity, field, {
			id: field === 'id' ? value : `_del_${value}`,
			__leo_delete__: field,
			__leo_delete_id__: value
		});
	}

	async.eachSeries(entities, (entity, entityDone) => {
		const field = entity.field || 'id';
		const table = transform.parseTable({ type: entity.type, entity: entity.name });
		const nks = (table && tableNks && tableNks[table]) || [];
		// Composite natural keys cannot be resolved to a single column; fall back.
		const nk = nks.length === 1 ? nks[0] : null;

		// Already keyed by the natural key — nothing to resolve.
		if (nk && field === nk) {
			ids.forEach(id => records.push(resolved(entity, nk, id)));
			return entityDone();
		}

		if (!nk || !client || typeof client.resolveDeleteKeys !== 'function') {
			ids.forEach(id => records.push(unresolved(entity, field, id)));
			return entityDone();
		}

		client.resolveDeleteKeys(table, field, nk, ids, (err, keys) => {
			if (err) {
				return entityDone(err);
			}
			(keys || []).forEach(key => records.push(resolved(entity, nk, key)));
			entityDone();
		});
	}, err => callback(err, records));
}

module.exports = deleteFanout;

const streamify = require('stream-array');

module.exports = streamify([
	{
		payload: {
			type: "dimension",
			table: "dim_foo_nk_single",
			data: {
				id: 2140
			}
		},
		id: "mock-dw-load-queue",
		event: "dw.load",
		event_source_timestamp: 1550198601299,
		timestamp: 1550198601299,
		eid: 0
	},
	{
		payload: {
			type: "dimension",
			table: "dim_foo_nk_single",
			data: {
				id: 2141
			}
		},
		id: "mock-dw-load-queue",
		event: "dw.load",
		event_source_timestamp: 1550198601299,
		timestamp: 1550198601299,
		eid: 0
	},
	{
		payload: {
			type: "dimension",
			table: "dim_foo_nk_named",
			data: {
				foo_id: "ABC123!@#"
			}
		},
		id: "mock-dw-load-queue",
		event: "dw.load",
		event_source_timestamp: 1550198601299,
		timestamp: 1550198601299,
		eid: 0
	},
	{
		payload: {
			type: "dimension",
			table: "dim_foo",
			data: {
				foo_pk1: "FOOABC123!@#",
				foo_pk2: "BARABC123!@#"
			}
		},
		id: "mock-dw-load-queue",
		event: "dw.load",
		event_source_timestamp: 1550198601299,
		timestamp: 1550198601299,
		eid: 0
	},
	{
		payload: {
			type: "fact",
			table: "fact_bar",
			data: {
				bar_id: "FOOBARD001",
				foo_pk1: "FOOABC123!@#",
				foo_pk2: "BARABC123!@#"
			}
		},
		id: "mock-dw-load-queue",
		event: "dw.load",
		event_source_timestamp: 1550198601299,
		timestamp: 1550198601299,
		eid: 0
	}
]);


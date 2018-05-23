module.exports = function() {
	return Promise.resolve({
		body: {
			session: {},
			settings: {
				table: "f_lead",
				id_column: "id"
			},
			data: {}
		},
		params: {
			querystring: {
				method: "range"
			}
		}
	});
};

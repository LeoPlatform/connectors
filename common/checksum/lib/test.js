var db1 = {};
var db2 = {};
[{
	id: 1,
	id2: 99,
	name: "Clint",
}, {
	id: 2,
	id2: 673,
	name: "Stephen",
}, {
	id: 3,
	id2: 234,
	name: "Francis",
}, {
	id: 4,
	id2: 98,
	name: "Joseph",
}, {
	id: 5,
	id2: 0,
	name: "Lance",
}, {
	id: 6,
	id2: 443,
	name: "Blaine",
}, {
	id: 7,
	id2: 2456,
	name: "Steve",
}].map(r => {
	db1[r.id] = r;
	db2[r.id2] = r;
});


require("../").basicConnector("connector_id", {
	id_column: "id"
}, {
	// Respond to start and end
	// Return Stream, Array, or a Hash
	batch: function(start, end, done) {
		console.log(start, end, this.settings);
		let data = [];
		let db = this.settings.id_column == "id2" ? db2 : db1;
		for (let i = start; i <= end; i++) {
			let v = db[i];
			if (v !== undefined) {
				data.push(v);
			}
		}
		//return Promise.resolve(data);
		done(null, data);
	},

	// Respond to start and end
	// Return Stream, Array, or a Hash
	individual: function(start, end) {

		console.log(start, end, this.settings);
		let data = [];
		let db = this.settings.id_column == "id2" ? db2 : db1;
		for (let i = start; i <= end; i++) {
			let v = db[i];
			if (v !== undefined) {
				data.push(v);
			}
		}
		return Promise.resolve(data);
	},

	// Respond to ids
	// Return Stream, Array
	sample: function(ids) {
		let data = [];
		let db = this.settings.id_column == "id2" ? db2 : db1;
		ids.map(id => {
			let v = db[id];
			if (v !== undefined) {
				data.push(v);
				console.log(v);
			}
		});

		return Promise.resolve(data);
	},

	// Respond to start and end -- options
	// Return object with min, max, total
	range: function(start, end) {
		console.log(start, end, this.settings);
		let min = null;
		let max = null;
		let total = 0;
		let db = this.settings.id_column == "id2" ? db2 : db1;
		Object.keys(db).map(id => {
			id = db[id][this.settings.id_column];
			if ((start == undefined || id >= start) && (end == undefined || id <= end)) {
				total++;
				if (min == null || id < min) {
					min = id;
				}
				if (max == null || id > max) {
					max = id;
				}
			}
		});
		return Promise.resolve({
			min,
			max,
			total
		});
	},

	// Responds to start, end, limit, reverse
	// Returns object with next, current
	nibble: function(start, end, limit, reverse) {
		console.log(start, end, this.settings);
		let db = this.settings.id_column == "id2" ? db2 : db1;
		let current = null;
		let next = null;
		let dir = 1;
		let ostart = start;
		let oend = end;
		if (reverse) {
			start = end;
			end = ostart;
			dir = -1;
		}
		let cnt = 0;
		for (let i = start; i >= ostart && i <= oend; i += dir) {
			let v = db[i];
			if (v !== undefined) {
				cnt++;
				if (cnt >= limit) {
					if (!current) {
						current = v[this.settings.id_column];
					} else {
						next = v[this.settings.id_column];
						break;
					}
				}
			}
		}

		return Promise.resolve({
			current,
			next
		});
	},

	// Called With data
	// Returns a session
	initialize: function(data) {
		return Promise.resolve({});
	},

	// Called With data
	destroy: function(data) {
		return Promise.resolve();
	},

	// Respond to ids
	// No return
	delete: function(ids) {
		let db = this.settings.id_column == "id2" ? db2 : db1;
		ids.map(id => {
			if (id in db) {
				delete db[id];
			}
		});
		return Promise.resolve();
	}
}).delete({
	//reverse: true,
	limit: 3,
	start: 0,
	end: 100000,
	ids: [1]
}).then(data => {
	console.log("cool", data);
}).catch(err => {
	console.log(err);
});
// .handler({
// 	params: {
// 		querystring: {
// 			method: "range"
// 		}
// 	},
// 	body: {
// 		session: {},
// 		settings: {
// 			id_column: "id",
// 		},
// 		data: {
// 			reverse: true,
// 			limit: 3,
// 			//start: 0,
// 			//end: 1,
// 			ids: [1, 5, 7, 99, 2456]
// 		}
// 	}
// }, {}, (err, data) => {
// 	console.log(err, data && data.response);
// });
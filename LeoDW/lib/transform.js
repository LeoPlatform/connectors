"use strict";

module.exports = {
	parseTable: function(obj) {
		if (obj.table) {
			return obj.table;
		} else if (obj.type) {
			if (obj.type == "fact") {
				return "f_" + obj.entity.toLowerCase().replace(/\s/g, '_');
			} else {
				return "d_" + obj.entity.toLowerCase().replace(/\s/g, '_');
			}
		} else {
			return null;
		}
	},
	parseValues: function(obj) {
		let outObj = {};
		let matches;
		for (var key in obj) {
			var value = obj[key];
			let columnName = key.toLowerCase();

			if (key.match(/^[A-Z]/)) {
				if (matches = key.toLowerCase().match(/^ts(:.*|$)/)) {
					columnName = key.replace(/^ts:?/i, '') + "_ts";
					if (columnName == "_ts") {
						columnName = "ts";
					}
					if (value) {
						try {
							outObj[columnName.toLowerCase()] = new Date(value).toISOString();
						} catch (err) {
							//ignore this field
						}
					}
				} else if (value && typeof value == "object") {
					for (var subkey in value) {
						var subvalue = value[subkey];
						outObj[subkey.toLowerCase()] = subvalue;
					}
				} else {
					let l = key.replace(/^[^:]*:/i, '').toLowerCase() + "_id";
					outObj[l] = value;
				}
			} else {
				if (typeof value == "string" && value.length > 100) {
					outObj[key.toLowerCase()] = value.slice(0, 100);
				} else {
					outObj[key.toLowerCase()] = value;
				}
			}
		}
		return outObj;
	}
};
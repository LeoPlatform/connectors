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
	parseValues: function(obj, dateformat) {
		let outObj = {};
		let matches;

		for (var key in obj) {
			var value = obj[key];
			if (key.match(/^[A-Z]/)) {
				let columnName = key.toLowerCase().replace(/[^a-z0-9\:]/g, '_');
				if (matches = columnName.match(/^ts(:.*|$)/)) {
					columnName = columnName.replace(/^ts:?/i, '') + "_ts";
					if (columnName == "_ts") {
						columnName = "ts";
					}
					if (value) {
						try {
							outObj[columnName] = dateformat(new Date(value));
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
					let l = columnName.replace(/^[^:]*:/i, '') + "_id";
					outObj[l] = value;
				}
			} else {
				let columnName = key.toLowerCase().replace(/[^a-z0-9]/g, '_');
				if (typeof value == "string" && value.length > 100) {
					outObj[columnName] = value.slice(0, 100);
				} else {
					outObj[columnName] = value;
				}
			}
		}
		return outObj;
	}
};
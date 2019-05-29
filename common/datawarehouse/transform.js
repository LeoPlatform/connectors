"use strict";

module.exports = {
	parseTable: function(obj) {
		if (obj.table) {
			return obj.table;
		} else if (obj.type) {
			try {
				if (obj.type == "fact" && obj.entity) {
					return "f_" + obj.entity.toLowerCase().replace(/\s/g, '_');
				} else if (obj.entity) {
					return "d_" + obj.entity.toLowerCase().replace(/\s/g, '_');
				} else {
					return null;
				}
			} catch (e) {
				console.log("Invalid Table to Parse", obj);
				return null;
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
				const maxStringLength = 4000;
				if (typeof value == "string" && value.length > maxStringLength) {
					outObj[columnName] = value.slice(0, maxStringLength);
				} else {
					outObj[columnName] = value;
				}
			}
		}
		return outObj;
	}
};
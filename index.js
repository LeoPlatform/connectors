module.exports = {
	createChangeTrackingObject: function (tableIdentifer, primaryKey) {
		let changeTracking = {};
		let idMap = {};
		let builder = {
			mapToDomainId: function (tableIdentifer, primaryKeyToTargetPrimaryKeyQuery) {
				idMap[tableIdentifer] = primaryKeyToTargetPrimaryKeyQuery == undefined ? true : primaryKeyToTargetPrimaryKeyQuery;
				return this;
			},
			trackTable: function (tableIdentifer, primaryKey) {
				changeTracking[tableIdentifer] = primaryKey;
				this.mapToDomainId(tableIdentifer);
				return this;
			},
			getDomainIdMappings: () => idMap,
			getTrackedTables: () => changeTracking
		}

		if (tableIdentifer && primaryKey) {
			builder.trackTable(tableIdentifer, primaryKey);
		}
		return builder;
	},
	createLoader: function (id, query) {
		let obj = {
			id: id,
			sql: query,
			joins: {}
		};
		return {
			get: () => obj,
			join: function (name, joinOnId, joinQuery, transform) {
				obj.joins[name] = {
					type: "one_to_one",
					on: joinOnId,
					sql: joinQuery,
					transform: transform
				};

				return this;
			},
			joinOneToMany: function (name, joinOnId, joinQuery, transform) {
				obj.joins[name] = {
					type: "one_to_many",
					on: joinOnId,
					sql: joinQuery,
					transform: transform
				};

				return this;
			}
		};
	}
}
module.exports = {
	"dim_foo_nk_single": {
		"structure": {
			"foo_key": "sk",
			"id": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo_nk_single",
		"label": "Foo Label",
		"isDimension": true
	},	
	"dim_foo_nk_named": {
		"structure": {
			"foo_key": "sk",
			"foo_id": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo_nk_named",
		"label": "Foo Label",
		"isDimension": true
	},	
	"dim_foo": {
		"structure": {
			"foo_key": "sk",
			"foo_pk1": {
				"type": "text",
				"nk": true
			},
			"foo_pk2": {
				"type": "text",
				"nk": true
			}
		},
		"identifier": "dim_foo",
		"label": "Foo Label",
		"isDimension": true
	},
	"fact_bar": {
		"structure": {
			"bar_id": {
				"type": "text",
				"nk": true
			},
			"foo_pk1": {
				"type": "text",
				"dimension": "dim_foo",
				"on": {
					"foo_pk1": "foo_pk1",
					"foo_pk2": "foo_pk2"
				},
				"dim_column": "foo_key"
			},
			"foo_pk2": "text",
		},
		"identifier": "fact_bar",
		"label": "Bar Label"
	}
}
;

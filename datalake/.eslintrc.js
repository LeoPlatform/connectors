module.exports = {
	"env": {
		"es6": true,
		"node": true,
		"mocha": true
	},
	"extends": "eslint:recommended",
	"parserOptions": {
		"ecmaVersion": 2019,
		"sourceType": "module"
	},
	"globals": {
		"BigInt": "readonly"
	},
	"rules": {
		"eol-last": ["error", "always"],
		"no-console": 0,
		"indent": [
			"error",
			"tab"
		],
		"semi": [
			"error",
			"always"
		],
		"no-unused-vars": ["error", { "vars": "all", "args": "after-used", "argsIgnorePattern": "^_", "varsIgnorePattern": "^_" }]
	}
};

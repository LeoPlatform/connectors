'use strict';
const merge = require('lodash.merge');
const beautify = require('js-beautify').js_beautify;
const fs = require('fs');

module.exports = {
	inquire: async function(utils) {
		let tableName = await utils.prompt('What type would you like entity table called?', 'Entities', /^[A-Za-z0-9]+$/);
		let type = await utils.prompt('What type would you like your ID to be in the Entities table? [S(string)|N(number)]', 'S', /^[SN]$/);
		let sourceQueue = await utils.prompt('What queue would you like to read?');
		let entityName = await utils.prompt('What is the name of the entity?', sourceQueue);
		let entityLoaderBotId = (entityName + '_entity_loader').toLowerCase();
		let entityProcessorBotId = (tableName + '_change_processor').toLowerCase();

		let replacements = {
			__source_queue__: sourceQueue,
			__Entities__: tableName,
			__bot01__: utils.properCaseTransform(entityLoaderBotId),
			__bot01_id__: entityLoaderBotId,
			__bot02__: utils.properCaseTransform(entityProcessorBotId),
			__bot02_id__: entityProcessorBotId,
			__id_parsed__: type == 'S' ? "payload.id.toString()" : "parseInt(payload.id)",
			__entity_id_type__: type,
			__Entities_Ref__: `${tableName}Table`
		};

		return replacements;
	},
	process: async function(utils, context) {
		await updateConfig(utils, context);
		await updateModules(utils, context);
	}
};

async function updateConfig(utils, context) {
	let configs = utils.findParentFiles(process.cwd(), "leo_config.js");

	if (!configs || !configs[0] || !configs[0].length) {
		throw new Error('leo_config.js not found in installation path.');
	}

	let dirPath = process.cwd().split('/');
	let lastIndex = dirPath.length - 1;

	if (lastIndex < 0) {
		throw new Error('Error while attempting to process setup.js. Cannot find valid directory.');
	}

	// add tables to leo_config.js
	let entityTableName = `${context.__Entities_Ref__}: process.env.${context.__Entities_Ref__},`;

	await utils.asyncReadFile(configs[0]).then(async data => {
		let configVars = !data.match(entityTableName) && entityTableName;

		// we already have tables defined in the config.
		if (!configVars) {
			return;
		}

		// insert the new config vars inside _global
		data = data.replace(/_global:\W*\{/, `_global: {${configVars}`);

		// format and write the file
		data = beautify(data, {
			indent_with_tabs: true
		});
		fs.writeFileSync(configs[0], data);
	}).catch(err => {
		console.log(err);
		throw new Error(`Unable to read ${configs[0]}`);
	});
}

/**
 * Update package.json with new modules
 * @param utils
 * @param context
 * @returns {Promise<void>}
 */
async function updateModules(utils, context) {
	let packages = utils.findParentFiles(process.cwd(), "package.json");

	if (!packages || !packages[0] || !packages[0].length) {
		throw new Error('package.json not found in installation path.');
	}

	let version = require("../../package.json").version;
	await utils.asyncReadFile(packages[0]).then(data => {
		let p = JSON.parse(data);

		p.dependencies = merge(p.dependencies || {}, {
			'leo-connector-entity-table': '>=' + version,
			'leo-aws': (p.dependencies && p.dependencies['leo-aws']) || ">=1.4.0"
		});

		fs.writeFileSync(packages[0], beautify(JSON.stringify(p)));
	});
}

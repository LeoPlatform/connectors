// let leo = require("leo-sdk");
// leo.aws.secrets = {
// 	getSecret: function(key) {
// 		return Promise.resolve({
// 			username: 'root',
// 			password: 'a',
// 			host: '127.0.0.1',
// 			dbname: 'datawarehouse'
// 		});
// 	}
// }

Object.assign(process.env, {
	dbsecret: "postgres_test_secret"
});

console.log(process.env.dbsecret);

module.exports = function() {
	return new Promise((resolve) => {
		setTimeout(resolve, 200);
	});
};

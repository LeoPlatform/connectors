var spawn = require('child_process').spawn;
var child = spawn(`/app/bin/maxwell`, [
	'--user', 'root',
	'--password=', 'a',
	'--host', '10.0.75.1',
	'--producer', 'stdout',
	'--log_level', 'ERROR'
]);

const leo = require("leo-sdk");
const ls = leo.streams;

let i = 0;
child.stdout.pipe(ls.through((obj, done) => {
	if (obj.toString().match(/^Using/)) {
		done(null);
	} else {
		done(null, obj);
	}
})).pipe(ls.parse()).pipe(ls.through((obj, done) => {
	i++;
	if (i % 10000 == 0) {
		console.log(i);
	}
	done(null, obj);
})).pipe(leo.write("mysqlbinlog", {
	useS3: true,
	chunk: {
		useS3Mode: true
	}
}));

child.stderr.on('data', function(data) {
	console.log('stderr: ' + data);
	//Here is where the error output goes
});
child.on('close', function(code) {
	console.log('closing code: ' + code);
	//Here you can get the exit code of the script
});
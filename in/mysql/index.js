var spawn = require('child_process').spawn;
var child = spawn(`/app/bin/maxwell`, [
	'--user', 'root',
	'--password=', 'a',
	'--host', '10.0.75.1',
	'--producer', 'stdout',
	'--log_level', 'ERROR'
]);

const leo = require("leo-sdk")({
	"region": "us-west-2",
	"kinesis": "Leo-KinesisStream-ATNV3XQO0YHV",
	"s3": "leoplatform",
	"firehose": "Leo-FirehoseStream-189A8WXE76MFS",
	"resources": {
		"LeoArchive": "Leo_archive",
		"LeoCron": "Leo_cron",
		"LeoEvent": "Leo_event",
		"LeoSettings": "Leo_settings",
		"LeoStream": "Leo_stream",
		"LeoSystem": "Leo_system"
	}
});
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
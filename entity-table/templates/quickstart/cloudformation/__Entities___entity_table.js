let cf = require("leo-aws/utils/cloudformation.js")();

module.exports = cf.add({
	"__Entities__EventMapping": {
		"Type": "AWS::Lambda::EventSourceMapping",
		"Properties": {
			"BatchSize": 500,
			"Enabled": true,
			"StartingPosition": "TRIM_HORIZON",
			"EventSourceArn": {
				"Fn::Sub": "${__Entities__.StreamArn}"
			},
			"FunctionName": {
				"Fn::Sub": "${__bot02__}"
			}
		}
	}
}).add({
	"__Entities__ChangesRole": {
		"Type": "AWS::IAM::Role",
		"Properties": {
			"AssumeRolePolicyDocument": {
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Principal": {
						"Service": [
							"lambda.amazonaws.com"
						],
						"AWS": {
							"Fn::Sub": "arn:aws:iam::${AWS::AccountId}:root"
						}
					},
					"Action": [
						"sts:AssumeRole"
					]
				}]
			},
			"ManagedPolicyArns": [
				"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
				{
					"Fn::ImportValue": {
						"Fn::Sub": "${LeoBus}-Policy"
					}
				}
			],
			"Policies": [{
				"PolicyName": "__Entities__",
				"PolicyDocument": {
					"Version": "2012-10-17",
					"Statement": [{
						"Effect": "Allow",
						"Action": [
							"dynamodb:Scan",
							"dynamodb:PutItem",
							"dynamodb:BatchWriteItem",
							"dynamodb:BatchGetItem",
							"dynamodb:UpdateItem",
							"dynamodb:Query"
						],
						"Resource": [{
							"Fn::Sub": "${__Entities__.Arn}"
						}]
					},
					{
						"Effect": "Allow",
						"Action": [
							"dynamodb:GetRecords",
							"dynamodb:GetShardIterator",
							"dynamodb:DescribeStream",
							"dynamodb:ListStreams"
						],
						"Resource": [{
							"Fn::Sub": "${__Entities__.StreamArn}"
						}]
					}
					]
				}
			}]
		}
	}
}).add(cf.dynamodb.table("__Entities__", {
	id: '__entity_id_type__',
	partition: 'S',
	autoscale: true,
	throughput: {
		read: 20,
		write: 20
	},
	stream: "NEW_AND_OLD_IMAGES"
}));

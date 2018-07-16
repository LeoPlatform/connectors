let cf = require("leo-aws/utils/cloudformation.js")({
	Resources: {
		"LeoEventMapping": {
			"Type": "AWS::Lambda::EventSourceMapping",
			"Properties": {
				"BatchSize": 500,
				"Enabled": true,
				"StartingPosition": "TRIM_HORIZON",
				"EventSourceArn": {
					"Fn::Sub": "${Entities.StreamArn}"
				},
				"FunctionName": {
					"Fn::Sub": "${____DIRNAMEP____ChangeProcessor}"
				}
			}
		},
		"LeoEntitiesChangesRole": {
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
					"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole", {
						"Fn::ImportValue": {
							"Fn::Sub": "${LeoBus}-Policy"
						}
					}
				],
				"Policies": [{
					"PolicyName": "Leo_Entities",
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
								"Fn::Sub": "${Entities.Arn}"
							}]
						}, {
							"Effect": "Allow",
							"Action": [
								"dynamodb:GetRecords",
								"dynamodb:GetShardIterator",
								"dynamodb:DescribeStream",
								"dynamodb:ListStreams"
							],
							"Resource": [{
								"Fn::Sub": "${Entities.StreamArn}"
							}]
						}]
					}
				}]
			}
		}
	}
});

module.exports = cf.add(cf.dynamodb.table("Entities", {
	partition: 'S',
	id: 'N',
	autoscale: true,
	throughput: {
		read: 50,
		write: 50
	},
	stream: "NEW_AND_OLD_IMAGES"
}));

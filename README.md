# Index
 * [Creating a checksum bot](#creating-a-checksum-bot)
    * [Prerequisites](#prerequisites)
    * [NPM Requirements](#npm-requirements)
    * [Available Connectors](#available-connectors)
	* [Create Database connectors](#create-database-connectors)
		1. [Create a secret key](#1-create-a-secret-key)
		2. [Create a database connector](#2-create-a-database-connector)
		3. [Deploy the connectors](#3-deploy-the-connectors)
	* [Create a checksum runner (bot) with database connectors](#create-a-checksum-runner-bot-with-database-connectors)
		1. [Add the required modules](#1-add-the-required-modules)
		2. [Connect to the master and slave connectors](#2-connect-to-the-master-and-slave-connectors)
		3. [Setup the checksum](#3-setup-the-checksum)
		4. [Configure the checksum bot package.json](#4-configure-the-checksum-bot-packagejson)
		5. [Edit your cloudformation.json](#5-edit-your-cloudformationjson)
		6. [Deploy the checksum runner](#6-deploy-the-checksum-runner)
		7. [Running the checksum](#7-running-the-checksum)
	* [Custom Connector](#custom-connector)
		* [Available handlers for a master connector](#available-handlers-for-a-master-connector)
			* [Required handlers](#required-handlers)
			* [Optional handlers](#optional-handlers)
		* [Available handlers for a slave connector](#available-handlers-for-a-slave-connector)
			* [Required handlers](#required-handlers-1)
			* [Optional handlers](#optional-handlers-1)
		* [Handlers](#handlers)
			* [Initialize](#initialize)
			* [Range](#range)
			* [Batch](#batch)
			* [Nibble](#nibble)
			* [Individual](#individual)
			* [Delete](#delete)
			* [Sample](#sample)
			* [Destroy](#destroy)
 * [Support](#support)
		

# Creating a checksum bot

#### Prerequisites
 * You know how to create bots in the Leo Platform. (https://github.com/LeoPlatform/cli)
 * You know how to create AWS permissions.
 * You know how to do AWS networking.
 
#### NPM requirements
 * leo-sdk: 1.1.0+
 * leo-connector-common: 1.1.2+
 * leo-connector-(mysql|postgres|sqlserver): 1.3.0+
 
#### Available Connectors
1. leo-connector-mysql:
`npm install leo-connector-mysql`
2. leo-connector-postgres:
`npm install leo-connector-postgres`
3. leo-connector-sqlserver:
`npm install leo-connector-sqlserver`
 
## Create Database connectors

### 1: Create a secret key
If using the AWS secrets manager, create secret keys for your databases. The secret names will be used in step 2.

### 2: Create a database connector
If you already have a connector setup for this database connection, skip this step.

Using the CLI, create a connector bot for each database you need to connect to. If one or more of your connections are
an endpoint or a database type we don't support, see the basicConnector section.
##### Syntax
```bash
leo-cli create leo-connector-{connector type} checksum {bot name}
```

##### Example
```bash
leo-cli create leo-connector-mysql checksum mysqlConnector
```

Now browse to your new bot (bots/mysqlConnector) and open up **package.json** and replace the `dbsecret` key name
with the one you created in AWS Secrets Manager.

If you are using a VPC for access to your database, or are using an AWS RDS instance, add the VpcConfig to the
**package.json* under config.leo object.
##### Example (config object only, from package.json)
```json
"config": {
    "leo": {
        "type": "bot",
        "memory": 256,
        "timeout": 300,
        "role": "ApiRole",
        "env": {
            "dbsecret": "database_secret_key_name"
        },
        "VpcConfig": {
            "SecurityGroupIds": [
                "sg-123456ab"
            ],
            "SubnetIds": [
                "subnet-abc12345",
                "subnet-def67890",
                "subnet-ghi45679"
            ]
        }
    }
}
```
Repeat this step for each master or slave database you will run a checksum against.

### 3: Deploy the connectors
In your service, be sure to install the NPM modules for the connectors you are using.

Now publish and deploy the bots.

Congratulations! You now have connectors setup to run a checksum. Next we'll need to create a checksum runner.

## Create a checksum runner (bot) with database connectors

#### 1. Add the required modules
```javascript
const leo = require('leo-sdk');
const checksum = require('leo-connector-common/checksum');
const moment = require('moment');
```

#### 2. Connect to the master and slave connectors
Use lambdaConnector to connect to the 2 database connectors you created in the previous section and build out the
data you want to compare between the 2 connectors.
For this example, I'm using a MySQL connector for the master, and the Postgres for the slave. We're going to compare id
and status from the orders tables in both databases. 
```javascript
exports.handler = function(event, context, callback) {
    let db1 = checksum.lambdaConnector('MySQL DB Lead checksum', process.env.mysql_lambda, {
        sql: `SELECT id, status FROM orders WHERE id __IDCOLUMNLIMIT__`,
        table: 'orders',
        id_column: 'id',
        key_column: 'primary'
    });
    let db2 = checksum.lambdaConnector('Postgres DB Lead checksum', process.env.postgres_lambda, {
        sql: `SELECT id, status FROM orders WHERE id __IDCOLUMNLIMIT__`,
        table: 'orders',
        id_column: 'id',
        key_column: 'primary'
    });
    
    // checksum code in step 3 (below) goes here
}
```

#### 3. Setup the checksum
Now create the checksum with parameters.
```javascript
let system = 'default';
checksum.checksum(system, event.botId, db1, db2, {
    stopOnStreak: 1750000, // Set the number of records that if the checksum finds in sequence that are identical, it will stop and mark itself as completed.
    stop_at: moment().add({minutes: 4}), // Lambda has a 5-minute limit, so we set this to 4 so the bot has time to cleanup. It will restart right after this and continue where it left off.
    limit: 20000, // the number of records to start comparing between the 2 databases.
    maxLimit: 500000, // If a "block" 20,000 or more records are identical, increase the comparison block size from limit to this max limit
    shouldDelete: false, // set this to true if you want records that exist in the slave database but not in master to be deleted.
    loadSize: 50000, // this is the recommended load size
    reverse: true, // Processes records from highest to lowest. Set to false to process from lowest to highest.
    sample: true, // 
    queue: { // this controls the queue where the ID's go that are marked as missing from the slave database
        name: event.destination, // queue name.
        transform: leo.streams.through((obj, done) => { // How to transform the ID's before sending into the queue.
            done(null, {
                Orders: obj.missing.concat(obj.incorrect)
            });
        })
    }
    //skipBatch: true, // only set to true if you need to 2 connectors to compare individual records insteadof batches
    //showOutput: false
})
.then(data=>{ console.log(data); callback()})
.catch(callback);
```

#### 4. Configure the checksum bot package.json
##### Example package.json
```json
{
    "name": "OrdersChecksum",
    "version": "1.0.0",
    "description": "Checksum for the Orders table",
    "main": "index.js",
    "directories": {
        "test": "test"
    },
    "scripts": {
        "test": "leo-cli test . "
    },
    "config": {
        "leo": {
            "type": "cron",
            "memory": 256,
            "timeout": 300,
            "role": "ApiRole",
            "env": {
                "mysql_lambda": {
                    "Fn::Sub": "${MysqlConnector}"
                },
                "postgres_lambda": {
                    "Fn::Sub": "${PostgresConnector}"
                }
            },
            "cron": {
                "settings": {
                    "source": "system:mysqlConnector",
                    "destination": "orderChanges",
                }
            },
            "time": "0 0 0 * * * "
        }
    }
}
```

#### 5. Edit your cloudformation.json
Your cloudformation will now need to be configured to be able to "invoke lambda".
Skip this step if you already have this set.

In your cloudformation.json, search for the configuration for the role you're using. In the package.json example above, we're using "ApiRole".
Find the ApiRole in the Resources:

```
  "Resources": {
    "ApiRole": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "AssumeRolePolicyDocument": [...],
        "ManagedPolicyArns": [...],
        "Policies": [...Add Invoke Lambda policy here...]
      }
    },
```

Add policies to invoke lambda, connect to kms, and secrets manager.
##### Example
```json
{
    "PolicyName": "Invoke_Lambda",
    "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": "lambda:*",
                "Resource": "*"
            }
        ]
    }
},
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "secretsmanager:ListSecrets",
            "Resource": "*"
        }
    ]
},
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "kms:GetParametersForImport",
                "kms:ListKeyPolicies",
                "kms:GetKeyRotationStatus",
                "kms:ListRetirableGrants",
                "kms:GetKeyPolicy",
                "kms:DescribeKey",
                "kms:ListResourceTags",
                "kms:ListGrants",
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:*:*:key/*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "kms:ListKeys",
                "kms:GenerateRandom",
                "kms:ListAliases",
                "kms:ReEncryptTo",
                "kms:ReEncryptFrom"
            ],
            "Resource": "*"
        }
    ]
}
```

#### 6. Deploy the checksum runner
Make sure the checksum runner is not in a VPC (No VpcConfig in the package.json). Publish and deploy the checksum runner.

#### 7. Running the checksum
You can either wait for the checksum to run from the cron time set, or you can force it to run through botmon.
Once the bot runs once, when you open it up in botmon, the checksum tab will appear and you can see the current status,
if it's running, or the results from the last run.

## Custom Connector
If one of your endpoints is not a database (e.g. API), or not a database we support, you can create a custom
connector using `basicConnector`. A custom connector differs from a database connector in that you don't create a
specific connector for it, instead you add this directly into the runner.
    
You can create a custom connector either as the master and use a database connector for the slave; use a database
connector for the master and a custom connector for the slave; or use a custom connect for both the master and the slave.
    
##### Example of the structure of a basic connector
```javascript
let customConnector = checksum.basicConnector('< Checksum name >', {
    id_column: 'id'
}, {
    // custom handlers go here
});
```

Now add the handlers to handle the data.
### Available handlers for a master connector
##### Required handlers
 * batch
 * individual
 * range
 * nibble
 * delete
 
##### Optional handlers
 * sample (required if sample is set to true)
 * initialize
 * destroy

### Available handlers for a slave connector
##### Required handlers
 * batch
 * individual
 * delete
 
##### Optional handlers
 * sample (required if sample is set to true)
 * initialize
 * destroy
 * range
 * nibble
 
### Handlers

##### Initialize
Called when checksum starts (does not include restarts after a lambda 5-minute timeout)
```javascript
/**
 * Called when checksum starts.
 * Used for situations such as when your endpoint requires authorization.
 * Called with data, return a session
 */
initialize: function(data) {
    return Promise.resolve({});
}
```
##### Range
Called after initialize. Range gets the max and min id's, as well as the total number of id's. This is stored in the
session until the checksum completes. Each restart of checksum after a lambda timeout will use the range stored in
the session.
```javascript
/**
 * @int start
 * @int end
 * @object options (optional)
 * @return object {min: int, max: int, total: int}
 */
// Respond to start and end -- options
// Return object with min, max, total
range: function(start, end) {
    let min = null;
    let max = null;
    let total = 0;
    
    /************************************************
    * Begin example code to get min, max, and total.*
    * This example loops through records returned   *
    * into “db” and creates a start and end from    *
    * the greatest and least id’s.                  *
    *************************************************/
    // db: object containing records to compare
    let db = [{id: 1, name: 'foo', etc: 'etc'}, {...}];
    Object.keys(db).map(id => {
        id = db[id][this.settings.id_column];
        if ((start === undefined || id >= start) && (end === undefined || id <= end)) {
            total++;
            if (min == null || id < min) {
                min = id;
            }
            if (max == null || id > max) {
                max = id;
            }
        }
    });
    /**********************************************
    * End example code to get min, max, and total *
    ***********************************************/
    
    // return a min, max and total
    return Promise.resolve({
        min,
        max,
        total
    });
}
```
##### Batch
Gets a chunk of data between a specified start and end to compare against a master or slave set of data.
```javascript
/**
 * Respond to a start and end, and build an array of data returned into “db”
 * 
 * @int start
 * @int end
 * @return mixed (Stream|Array|hash)
 */
batch: function(start, end) {
    let data = [];
    // db: object containing records to compare
    let db = [{id: 1, name: 'foo', etc: 'etc'}, {...}];
    
    /***********************************************************************************
     * Example code to put together an array of data using the data returned from “db” *
     ***********************************************************************************/
    for (v of db) {
        data.push(v);
    }
    
    /**********************************************************************************************************
    * Alternatively, if you cannot pass in a start and end and just get a chunk of data back, build an array *
    * with the data having id’s between start and end                                                         *
    ***********************************************************************************************************/
    for (let i = start; i <= end; i++) {
        if (typeof db[i] !== 'undefined') {
            data.push(db[i]);
        }
    }
    
    // return the array of data
    return Promise.resolve(data);
}
```
##### Nibble
```javascript
/**
 * Nibble handler: Uses a start, end, limit, and reverse; and gives a “next” and “current” to continue checking data
 * @int start
 * @int end
 * @int limit
 * @bool reverse
 */
// Responds to start, end, limit, reverse
// Returns object with next, current
nibble: function(start, end, limit, reverse) {
    // db: object containing records to compare
    let db = [{id: 1, name: 'foo', etc: 'etc'}, {...}];
    let current = null;
    let next = null;
    let dir = 1;
    let ostart = start;
    let oend = end;
    if (reverse) {
        start = end;
        end = ostart;
        dir = -1;
    }
    let cnt = 0;
    for (let i = start; i >= ostart && i <= oend; i += dir) {
        if (typeof db[i] !== undefined) {
            let v = db[i];
            cnt++;
            if (cnt >= limit) {
                if (!current) {
                    current = v[this.settings.id_column];
                } else {
                    next = v[this.settings.id_column];
                    break;
                }
            }
        }
    }

    return Promise.resolve({
        current,
        next
    });
}
```
##### Individual
The code required is the same as “batch” above. If you have created batch, just call batch in the return and you're done here.
If you don't already have batch, follow the example for batch, but use individual.
```javascript
individual: function(start, end) {
    return this.batch(start, end);
}
```

##### Delete
Delete records in the slave database that do not exist in the master database.
This only runs if `shouldDelete` is set to true.
```javascript
delete: function(ids) {
    // db: object containing records to compare
    let db = [{id: 1, name: 'foo', etc: 'etc'}, {...}];
    ids.map(id => {
        if (id in db) {
            delete db[id];
        }
    });
    return Promise.resolve();
}
```

##### Sample
Used to return a sample of ID that are different between the master and slave.
```javascript
// Respond to ids
// Return Stream, Array
sample: function(ids) {
    let data = [];
    // db: object containing records to compare
    let db = [{id: 1, name: 'foo', etc: 'etc'}, {...}];
    ids.map(id => {
        let v = db[id];
        if (v !== undefined) {
            data.push(v);
            console.log(v);
        }
    });

    return Promise.resolve(data);
}
```
##### Destroy
Destroy runs once on checksum completion. Use this if you need to shutdown a session or add additional logging.
```javascript
destroy: function(data) {
    return Promise.resolve();
}
```

## Support
Want to hire an expert, or need technical support? Reach out to the Leo team: https://leoinsights.com/contact

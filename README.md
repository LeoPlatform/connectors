# Creating a checksum bot:

## Create Database connectors:

#### Assumptions:
For this tutorial, it is assumed that you know how to create bots in the Leo Platform. If you don't, check out the README at: https://github.com/LeoPlatform/cli

### 1: Create a master database connector.
If you already have a connector setup for this database connection, skip this step.

Create a new bot with an index.js and package.json.
In the index.js file, use the proper connector for your database type:
(i.e. leo-connector-sqlserver, leo-connector-postgres or leo-connector-mysql)

Example **index.js** using leo-connector-mysql:
```javascript
// use the connector for your database type:
const connector = require('leo-connector-mysql');

module.exports = connector.checksum({
    host: process.env.DB_HOST, // use server instead of host when using sqlserver. 
    user: process.env.DB_USER,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME,
    password: process.env.KMS_DB_PASSWORD
});
```

Example **package.json**: (replace the name with your connector type)
```json
{
    "name": "dw-mysql-checksum-connector",
    "version": "1.0.0",
    "description": "MySQL connector for the Data Warehouse checksum",
    "main": "index.js",
    "directories": {
        "test": "test"
    },
    "scripts": {
        "test": "leo-cli test . "
    },
    "config": {
        "leo": {
            "type": "bot",
            "memory": 256,
            "timeout": 300,
            "role": "ApiRole",
            "env": {
                "DB_HOST": "dbhost.domain.com",
                "DB_PORT": 3306,
                "DB_NAME": "mydbname",
                "DB_USER": "mydbuser",
                "DB_PASSWORD": "mySuperSecretPassword"
            }
        }
    }
}
```

If you are using a VPC for access to your database, or are using an AWS RDS instance, add the VpcConfig to the config.leo object (replace the id's with those from your VPC):
```json
"config": {
    "leo": {
        "type": "bot",
        "memory": 256,
        "timeout": 300,
        "role": "ApiRole",
        "env": {
            "DB_HOST": "dbhost.domain.com",
            "DB_PORT": 3306,
            "DB_NAME": "mydbname",
            "DB_USER": "mydbuser",
            "DB_PASSWORD": "mySuperSecretPassword"
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

For MySQL and Postgres, we need to dynamically add the node modules to the package.json.
For MySQL, inside the config.leo object, add (make sure ../../ is the path to where your node_modules/mysql2 is located, relative to the connector bot):
```json
"build": {
    "include": [
        "../../node_modules/mysql2"
    ]   
}
```

For Postgres, inside the config.leo object, add (make sure ../../ is the path to where your node_modules/pg is located, relative to the connector bot):
```json
"build": {
    "include": [
        "../../node_modules/pg",
        "../../node_modules/pg-format"
    ]   
}   
```

### 2: Create a slave database connector.
This will be your data warehouse or anything you want to compare against the master database.
**Repeat step 1** for this bot but with the slave database connection information.
If your slave is not a database but an endpoint, see the custom URL connector section (in-progress).

### 3: Deploy the bots
In your service, be sure to install the NPM modules for the connectors you are using.
#### Available Connectors:
1. leo-connector-mysql:
`npm install leo-connector-mysql`
2. leo-connector-postgres:
`npm install leo-connector-postgres`
3. leo-connector-sqlserver:
`npm install leo-connector-sqlserver`

Now publish and deploy the bots.

Congratulations! You now have connectors setup to run a checksum. Next we'll need to create a checksum runner.

## Create a checksum runner (bot)

#### 1. Add the required modules:
```javascript
const leo = require('leo-sdk');
const checksum = require('leo-connector-common/checksum');
const moment = require('moment');
```

#### 2. Connect to the master and slave connectors.
Use lambdaConnector to connect to the 2 database connectors you created in the previous section and build out the
data you want to compare between the 2 connectors.
For this example, I'm using a MySQL connector for the master, and the Postgres for the slave. We're going to compare id
and status from the orders tables in both databases. 
```javascript
exports.handler = function(event, context, callback) {
    let db1 = checksum.lambdaConnector('MySQL DB Lead checksum', process.env.mysql_lambda, {
        sql: `SELECT id, status FROM orders __IDCOLUMNLIMIT__`,
        table: 'orders',
        id_column: 'id',
        key_column: 'primary'
    });
    let db2 = checksum.lambdaConnector('Postgres DB Lead checksum', process.env.postgres_lambda, {
        sql: `SELECT id, status FROM orders __IDCOLUMNLIMIT__`,
        table: 'orders',
        id_column: 'id',
        key_column: 'primary'
    });
    
    // checksum code in step 3 (below) goes here
}
```

#### 3. Setup the checksum.
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
Example package.json:
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

If your policy doesn't allow you to invoke lambda, modify, or add a new policy to the Policies array.
Example:
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
}
```

#### 6. Deploy
Publish and deploy the checksum runner. Make sure the checksum runner is not in a VPC.

#### 7. Running the checksum
You can either wait for the checksum to run from the cron time set, or you can force it to run through botmon.
Once the bot runs once, when you open it up in botmon, the checksum tab will appear and you can see the current status,
if it's running, or the results from the last run.

# Deploying a checksum runner:

### Step 1: Create a master database connector.
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
	password: process.env.KMS_DB_PASSWORD,
	connectTimeout: 20000 // set the connection timeout to 20 seconds
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

### Step 2: Create a slave database connector.
This will be your data warehouse or anything you want to compare against the master database.
**Repeat step 1** for this bot but with the slave database connection information.
If your slave is not a database but an endpoint, see the custom URL connector section (in-progress).

### Step 3: Deploy the bots
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


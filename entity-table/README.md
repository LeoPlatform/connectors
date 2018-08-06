LEO Quick Start Guide: https://github.com/LeoPlatform/Leo

# Index
 * [Creating a checksum bot](#creating-a-checksum-bot)

# Working with aggregate data

#### NPM Requirements
 * leo-connector-entity-table: 2.1.0+

## Step 1: Loading data into the entities table
Create a new bot to load entities from a queue:
```javascript
const config = require('leo-config');
const entityTable = require('leo-connector-entity-table');
const queueName = "example_queue_1";

// parameters:
// entity table name
// queue name to read data from
// function to transform the data for insertion into the entities table
entityTable.loadFromQueue(config.entityTableName, queueName, payloadTransform, {
    botId: context.botId,
    batchRecords: 25, 
    merge: false
}).then(() => {
    console.log(`Completed. Remaining Time:`, context.getRemainingTimeInMillis());
    callback();
}).catch(callback);

const payloadTransform = (payload, hash) => {
    // create a hash from the payload id and queueName. This expects payload to have an id.
    // this will create a string that looks like: `${queueName}-[0-9]`
    let hashed = hash(queueName, payload.id);

    // if you have created the entity type id as a string, make sure to convert the id to a string or it will fail.
    // otherwise convert to integer (else it will fail as well).
    let id = payload.id.toString();
    // let id = parseInt(payload.id);

    // The entity table will be filled out with the payload
    return Object.assign({}, payload, {
        id: id, 
        partition: hashed
    });
};
```
In the package.json, be sure to add the entity table name and set the trigger to be the queue you’re reading from.
Example package.json:
```json
{
    "name": "1-SampleEntityLoader",
    "version": "1.0.0",
    "description": "Takes data from a queue and loads into the entity table (DynamoDB)",
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
            "memory": 128,
            "timeout": 300,
            "role": "LeoEntitiesChangesRole",
            "env": {
                "entityTableName": {
                    "Fn::Sub": "${Entities}"
                }   
            },  
            "cron": {
                "settings": {}, 
                "triggers": [
                    "example_queue_1"
                ]   
            }   
        }   
    }   
}
```

## Step 2: Write entity changes to a queue
If you already have a entity table processor bot setup, you can skip this step.
For this step, you'll need to have 1 entity change processor that contains a single line:
```javascript
exports.handler = require("leo-connector-entity-table").tableProcessor;
```
And make sure the bot has the proper role:
```json
{
  "name": "2-EntityChangeProcessor",
  "version": "1.0.0",
  "description": "Reads from DynamoDB entity table and writes to a queue with entity changes",
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
      "timeout": 30, 
      "role": "LeoEntitiesChangesRole",
      "env": {}, 
      "cron": {
        "settings": {}
      }   
    }   
  }
}
```
## Step 3: Aggregate data and load it into the aggregations table
```javascript
const leo = require("leo-sdk");
const config = require("leo-config");
const ls = leo.streams;
const agg = require("leo-connector-entity-table/lib/aggregations.js");
// give your entity a name. This will be used in the aggregations table to prefix id and will be appended to bucket.
const entity = 'example';

exports.handler = require("leo-sdk/wrappers/cron.js")((event, context, callback) => {
    let source = Object.assign({
        // this should be the queue name you've been using all along, but with “_changes” appended.
        source: "example_queue_1_changes"
    }, event).source;

    let stats = ls.stats(context.botId, source);
    ls.pipe(
        leo.read(context.botId, source)
        , stats
        // run the aggregator an the aggreations table, pass in the entity name, and transform the payload
        , agg.aggregator(config.aggregationTableName, entity, payload => [transformChanges(payload)])
        , err => {
            if (err) {
                callback(err);
            } else {
                let statsData = stats.get();
                stats.checkpoint((err) => {
                    if (err) {
                        return callback(err);
                    }   
                    if (statsData.units > 0) {
                        leo.bot.checkpoint(context.botId, `system:dynamodb.${config.aggregationTableName.replace(/-[A-Z0-9]{12}$/, "")}.${entity}`, {
                            type: "write",
                            eid: statsData.eid,
                            records: statsData.units,
                            started_timestamp: statsData.started_timestamp,
                            ended_timestamp: statsData.ended_timestamp,
                            source_timestamp: statsData.source_timestamp
                        }, () => {
                            callback();
                        }); 
                    } else {
                        callback();
                    }   
                }); 
            }   
        }
    ); 
});

const transformChanges = payload => ({
    // will be used to prefix id and append to bucket in the aggregations table
    entity: entity,
    id: payload.id,
    aggregate: {
        timestamp: payload.updated,
        // select the bucket(s) you would like to put the data into.
        // available options: alltime, quarterly, monthly, weekly, daily, hourly, minutely, or you can type in a custom dateformat
        // you can aggregate data into more than one timeframe.
        // If you have alltime, every time data is pushed, it will be stored with an alltime postfix
        // If you have quarterly, only 1 data set per quarter will be saved for this object
        // If you have Monthly, only 1 data set per month will be saved for this object
        // etc… for weekly, daily, hourly, minutely.
        buckets: ["alltime", "hourly", "minutely"]
    },  
    data: {
        // always run first on the ID
        id: agg.first(Date.now(), payload.id),
        // store the last payload
        payload: agg.last(Date.now(), payload)
    }   
});
```

## Support
Want to hire an expert, or need technical support? Reach out to the Leo team: https://leoinsights.com/contact

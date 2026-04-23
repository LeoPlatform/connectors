# Documentation
Quick Start Guide: https://github.com/LeoPlatform/Leo

Documentation for the connectors can be found here: https://github.com/LeoPlatform/connectors

# SQL Server Change Tracking

Change tracking must first be enabled at the database level:

```sql
ALTER DATABASE <databaseName>
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON)
```

Enable change tracking for each table you want tracked:

```sql
ALTER TABLE <tableName>
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = OFF)
```

See tables with change tracking enabled:

```sql
SELECT so.name AS trackedTable
FROM sys.internal_tables it
INNER JOIN sys.dm_db_partition_stats ps ON ps.object_id = it.object_id
LEFT JOIN sys.objects so ON so.object_id = it.parent_object_id
WHERE it.internal_type IN (209, 210)
AND ps.index_id < 2
AND so.name IS NOT NULL
```

# Docker

1. __Build the fullstack docker container__

    - ```docker build --no-cache --build-arg UID=1000 --build-arg GID=1000 -t leo-node:node docker/node/```

2. __Up the fullstack container__

    - __Attached Mode__:
    
        - ```docker-compose -f docker/node/docker-compose.yml up```
    
    - __Detached Mode__:
    
        - ```docker-compose -f docker/node/docker-compose.yml up -d```
    
3. __Obtain an interactive shell in the container__

    - ```docker exec -it node_leo-node_1 sh```

4. __From the shell you can run ```npm test```; test the CDC bot, etc__

    -  ```npm install```

    -  ```npm test```

# Support
Want to hire an expert, or need technical support? Reach out to the Leo team: https://leoinsights.com/contact
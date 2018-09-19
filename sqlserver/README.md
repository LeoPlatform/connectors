# Documentation
Quick Start Guide: https://github.com/LeoPlatform/Leo

Documentation for the connectors can be found here: https://github.com/LeoPlatform/connectors

# SQL Server Change Tracking

Change tracking must first be enabled at the database level:

```sql
ALTER DATABASE <databaseName>
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON)```
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

# Support
Want to hire an expert, or need technical support? Reach out to the Leo team: https://leoinsights.com/contact
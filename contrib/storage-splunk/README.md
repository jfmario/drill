# Drill Connector for Splunk
This plugin enables Drill to query Splunk clusters. 

## Configuration
To connect Drill to Splunk, create a new storage plugin with the following configuration:

To successfully connect, Splunk uses port `8089` for interfaces.  This port must be open for Drill to query Splunk data. 

```json
{
   "type":"splunk",
   "username": "admin",
   "password": "changeme",
   "hostname": "localhost",
   "port": 8089,
   "earliestTime": "-14d",
   "latestTime": "now",
   "enabled": false
}
```

## Understanding Splunk's Data Model
Splunk's primary use case is analyzing event logs with a timestamp. As such, data is indexed by the timestamp, with the most recent data being indexed first.  By default, Splunk
 will sort the data in reverse chronological order.  Large Splunk installations will put older data into buckets of hot, warm and cold storage with the "cold" storage on the
  slowest and cheapest disks. 
  
With this understood, it is **very** important to put time boundaries on your Splunk queries. The Drill plugin allows you to set default values in the configuration such that every
 query you run will be bounded by these boundaries.  Alternatively, you can set the time boundaries at query time.  In either case, the best performance will be achieved when
  you query the least amount of the most recent data.
  
### Selecting Fields
When you execute a query in Drill for Splunk, the fields you select are pushed down to Splunk. Therefore, it will always be more efficient to explicitly specify fields to push
 down to Splunk rather than using `SELECT *` queries.

  
### Sorting Results
Due to the nature of Splunk indexes, data will always be returned in reverse chronological order. Thus, sorting is not necessary if that is the desired order.



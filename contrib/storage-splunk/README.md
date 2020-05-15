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

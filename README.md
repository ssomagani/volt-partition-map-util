# volt-partition-map-util
Utility to get mapping of a partition column value --> PartitionId:SiteId@Hostname on a running cluster. 

## Requirements
This utility uses the call `@Statistics PLANNER 0` to get the initial list of partitions, sites, and hosts. But this list is built lazily only when a partition is actually accessed. If you see the value 'null' for 'SiteId@Hostname', that means that partition hasn't been accessed yet. So, this utility is best run after some workload that covers all partitions has been run already. 

## Running instructions
$ ./gradlew run --args='server.ip.address'

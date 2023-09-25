# Zarka
A Key-Value store that is inspired by Cassandra architecture.

# Archticture
Zarka have two kinds of applications:
- Zarka Server: a node that holds data partitions and replicas.
- Zarka Client: a command line interface where user can send two types of requests:
  - add( key, value)
  - get( key )
  Both key and value are string values.
  Zarka Client will pick any Zarka Server nodes at random and it will be the coordinator to execute his request.

# Features
1. Commit logs are used to record writes to disk as a crash recovery mechanism.
2. Sorted String Tables (SSTables) provide permanent on-disk storage.
3. Consistent Hashing will be used as the partition rebalancing strategy.
4. Leaderless replication will be used with configurable quorum sizes.

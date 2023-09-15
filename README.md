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
1. LSM-Tree needs to be used as your only storage data structure.
2. Consistent Hashing will be used as your partition rebalancing strategy (Only adding a new node should be supported).
3. Leaderless replication will be used with configurable quorum sizes.

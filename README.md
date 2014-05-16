AmazonSimpleDynamoOnAndroid
===========================

Implement a Dynamo-style key-value storage and the main goal is to provide both availability and linearizability at the same time. There are three main pieces to implement: 1) Partitioning, 2) Replication, and 3) Failure handling.

Dynamo paper: http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

The following is a guideline for the content provider based on the design of Amazon Dynamo:

Membership :
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

Request routing :
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
Under no failures, all requests are directly forwarded to the coordinator, and the coordinator should be in charge of serving read/write operations.

Quorum replication :
For linearizability, you can implement a quorum-based replication used by Dynamo.
Note that the original design does not provide linearizability. You need to adapt the design.
The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
Both the reader quorum size R and the writer quorum size W should be 2.
The coordinator for a get/put request should always contact other two nodes and get the votes.
For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy.
For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.

Chain replication :
Another replication strategy you can implement is chain replication, which provides linearizability.
If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write.
A read operation always comes to the last partition and reads the value from the last partition.

Failure handling :
Handling failures should be done very carefully because there can be many corner cases to consider and cover.
Just as the original Dynamo, each request can be used to detect a node failure.

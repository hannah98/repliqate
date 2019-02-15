# repliqate

**repliqate** is a daemon for replicating rows from a SQL database to a Kafka queue. It "listens" for SQL changes in append-only workflows and publishes new rows as JSON-serialized messages in a Kafka topic.

repliqate can use any flavor of SQL database with a [supported SQLAlchemy engine](https://docs.sqlalchemy.org/en/latest/core/engines.html) as input. Some databases (like SQLite) do not provide interfaces for listening to changes in real-time across process boundaries. repliqate "listens" for changes by regularly querying the source database, maintaining state/position across multiple invocations. For this reason, repliqate is suitable only for append-only write patterns.

## Operation

repliqate will regularly query the SQL database (specified as a URI) for new rows. It assumes that the replication source table has a monotonically increasing integer primary key, which is used to keep track of the most recently replicated row on each invocation. Thus, on each invocation, repliqate publishes a *batch* of rows (with a configurable size limit) that follow those from the last invocation (by ascending primary key value).

repliqate itself is stateless since it records the most recent primary key offset value in an external persistent key-value store (Redis). It is also reentrant; it may be stopped and started arbitrarily without corrupting replication state.

Note, however, that row replication is *not* idempotent. The Kafka producer may publish the same row more than once due to an external Kafka failure or a failure of repliqate's persistent offset store. For this reason, every published message includes a checksum (deterministically hashed from the row's data) which consumers should use to verify that the row has not already been processed.

## Usage

Download a release tarball from the internal object store under `/deploy/repliqate`. Create a configuration file to customize runtime behavior; an example is provided at `config.example.yaml`.

```bash
$ ./env/bin/repliqate --config /path/to/config.yaml
# Will run indefinitely
# Optionally add more logging verbosity with -v (repeated up to 3 times)
```

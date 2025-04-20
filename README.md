[![main](https://github.com/flowerinthenight/juno/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/juno/actions/workflows/main.yml)

```sql
-- Create the topics table:
CREATE TABLE juno_topics (
    TopicName STRING(MAX) NOT NULL,
    Created TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
    Updated TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
) PRIMARY KEY(TopicName);

-- Create the subscriptions table:
CREATE TABLE juno_subscriptions (
    TopicName STRING(MAX) NOT NULL,
    SubscriptionName STRING(MAX) NOT NULL,
    AcknowledgeTimeout INT64 NOT NULL DEFAULT (60),
    AutoExtend BOOL NOT NULL DEFAULT (TRUE),
    Created TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
    Updated TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
) PRIMARY KEY(TopicName, SubscriptionName),
INTERLEAVE IN PARENT juno_topics ON DELETE CASCADE;

-- Create the messages table:
CREATE TABLE juno_messages (
    TopicName STRING(MAX) NOT NULL,
    Id STRING(MAX) NOT NULL,
    Payload STRING(MAX),
    Attributes STRING(MAX),
    Acknowledged BOOL NOT NULL DEFAULT (FALSE),
    Created TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
    Updated TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
) PRIMARY KEY(TopicName, Id),
INTERLEAVE IN PARENT juno_topics ON DELETE CASCADE;
```

```bash
# Build:
$ cd <clone>/juno/
$ cargo build

# Run locally (1st instance):
$ RUST_LOG=info ./target/debug/juno \
  --id 0.0.0.0:8080 \
  --db projects/p/instances/i/databases/db \
  --table {tablename}

# Run 2nd instance:
$ RUST_LOG=info ./target/debug/juno \
  --id 0.0.0.0:8081 \
  --db projects/p/instances/i/databases/db \
  --table {tablename}
  --api 0.0.0.0:9091

# Run 3rd instance:
$ RUST_LOG=info ./target/debug/juno \
  --id 0.0.0.0:8082 \
  --db projects/p/instances/i/databases/db \
  --table {tablename}
  --api 0.0.0.0:9092
```

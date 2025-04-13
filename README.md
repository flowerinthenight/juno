[![main](https://github.com/flowerinthenight/juno/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/juno/actions/workflows/main.yml)

```sql
-- Create the topics table:
CREATE TABLE topics (
    TopicName STRING(MAX) NOT NULL,
    Created TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
    Updated TIMESTAMP OPTIONS (
        allow_commit_timestamp = true
    ),
) PRIMARY KEY(TopicName);

-- Create the subscriptions table:
CREATE TABLE subscriptions (
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
INTERLEAVE IN PARENT zzz_topics ON DELETE CASCADE;

-- Create the messages table:
CREATE TABLE messages (
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
INTERLEAVE IN PARENT zzz_topics ON DELETE CASCADE;
```

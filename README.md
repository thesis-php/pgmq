# pgmq

Non-blocking php client for [pgmq](https://github.com/pgmq/pgmq). See the extension [installation guide](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md).

## Installation

```shell
composer require thesis/pgmq
```

## Why is almost all the API functional?

Since you most likely expect exactly-once semantics from a database-based queue, all requests — sending or processing business logic with message acknowledgments — must be transactional.
And the transaction object is short-lived: it cannot be used after `rollback()` or `commit()`, so it cannot be made a dependency.
That's why all the API is built on functions that take `Amp\Postgres\PostgresLink` as their first parameter, which can be either a transaction object or just a connection.
And only the consumer accepts `Amp\Postgres\PostgresConnection`, because it itself opens transactions for reading and acknowledging messages transactionally.

## Contents
 - [Create queue](#create-queue)
 - [Create unlogged queue](#create-unlogged-queue)
 - [Create partitioned queue](#create-partitioned-queue)
 - [List queues](#list-queues)
 - [List queue metrics](#list-queue-metrics)
 - [List queue metadata](#list-queue-metadata)
 - [Drop queue](#drop-queue)
 - [Purge queue](#purge-queue)
 - [Send message](#send-message)
 - [Send message with relative delay](#send-message-with-relative-delay)
 - [Send message with absolute delay](#send-message-with-absolute-delay)
 - [Send batch](#send-batch)
 - [Send batch with relative delay](#send-batch-with-relative-delay)
 - [Send batch with absolute delay](#send-batch-with-absolute-delay)
 - [Read message](#read-message)
 - [Read batch](#read-batch)
 - [Pop message](#pop-message)
 - [Read batch with poll](#read-batch-with-poll)
 - [Set visibility timeout](#set-visibility-timeout)
 - [Archive message](#archive-message)
 - [Archive batch](#archive-batch)
 - [Delete message](#delete-message)
 - [Delete batch](#delete-batch)
 - [Enable notify insert](#enable-notify-insert)
 - [Disable notify insert](#disable-notify-insert)
 - [Bind topic](#bind-topic)
 - [Unbind topic](#unbind-topic)
 - [Send topic](#send-topic)
 - [Send topic with delay](#send-topic-with-delay)
 - [Test routing](#test-routing)
 - [Validate routing key](#validate-routing-key)
 - [Validate topic pattern](#validate-topic-pattern)
 - [Read grouped](#read-grouped)
 - [Read grouped round-robin](#read-grouped-round-robin)
 - [Read grouped head](#read-grouped-head)
 - [Read grouped with poll](#read-grouped-with-poll)
 - [Read grouped round-robin with poll](#read-grouped-round-robin-with-poll)
 - [Create FIFO index](#create-fifo-index)
 - [Create FIFO index for all queues](#create-fifo-index-for-all-queues)
 - [Consume messages](#consume-messages)

### Create queue

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
```

### Create unlogged queue

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createUnloggedQueue($pg, 'events');
```

### Create partitioned queue

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createPartitionedQueue(
    pg: $pg,
    queue: 'events',
    partitionInterval: 10000,
    retentionInterval: 100000,
);
```

### List queues

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

foreach (Pgmq\listQueues($pg) as $queue) {
    $md = $queue->metadata();
    var_dump($md);
}
```

### List queue metrics

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

foreach (Pgmq\metrics($pg) as $metrics) {
    var_dump($metrics);
}
```

### List queue metadata

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

foreach (Pgmq\listQueueMetadata($pg) as $md) {
    var_dump($md);
}
```

### Drop queue

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$queue->drop();
```

### Purge queue

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
var_dump($queue->purge());
```

### Send message

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageId = $queue->send(new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'));
```

### Send message with relative delay

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageId = $queue->send(
    new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'),
    TimeSpan::fromSeconds(5),
);
```

### Send message with absolute delay

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageId = $queue->send(
    new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'),
    new \DateTimeImmutable('+5 seconds'),
);
```

### Send batch

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageIds = $queue->sendBatch([
    new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'),
    new Pgmq\SendMessage('{"id": 2}', '{"x-header": "x-value"}'),
]);
```

### Send batch with relative delay

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageIds = $queue->sendBatch(
    [
        new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'),
        new Pgmq\SendMessage('{"id": 2}', '{"x-header": "x-value"}'),
    ],
    TimeSpan::fromSeconds(5),
);
```

### Send batch with absolute delay

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messageIds = $queue->sendBatch(
    [
        new Pgmq\SendMessage('{"id": 1}', '{"x-header": "x-value"}'),
        new Pgmq\SendMessage('{"id": 2}', '{"x-header": "x-value"}'),
    ],
    new \DateTimeImmutable('+5 seconds'),
);
```

### Read message

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->read(TimeSpan::fromSeconds(20));
```

### Read batch

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->readBatch(10, TimeSpan::fromSeconds(20));
```

### Pop message

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->pop();
```

### Read batch with poll

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messages = $queue->readPoll(
    batch: 10,
    maxPoll: TimeSpan::fromSeconds(5),
    pollInterval: TimeSpan::fromMilliseconds(250),
);
```

### Set visibility timeout

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->read();

if ($message !== null) {
    // handle the message

    $queue->setVisibilityTimeout($message->id, TimeSpan::fromSeconds(10));
}
```

### Archive message

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->read();

if ($message !== null) {
    $queue->archive($message->id);
}
```

### Archive batch

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messages = [...$queue->readBatch(5)];

if ($messages !== []) {
    $queue->archiveBatch(array_map(
        static fn(Pgmq\Message $message): int => $messages->id),
        $messages,
    );
}
```

### Delete message

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$message = $queue->read();

if ($message !== null) {
    $queue->delete($message->id);
}
```

### Delete batch

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$messages = [...$queue->readBatch(5)];

if ($messages !== []) {
    $queue->deleteBatch(array_map(
        static fn(Pgmq\Message $message): int => $messages->id),
        $messages,
    );
}
```

### Enable notify insert

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$channel = $queue->enableNotifyInsert(); // postgres channel to listen is returned
```

### Disable notify insert

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'events');
$queue->disableNotifyInsert();
```

### Bind topic

Bind a queue to a topic pattern. Messages sent with a routing key matching the pattern will be delivered to the queue.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'emails');
Pgmq\bindTopic($pg, 'notifications.*', $queue->name);
```

### Unbind topic

Remove a queue binding from a topic pattern.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

Pgmq\unbindTopic($pg, 'notifications.*', 'emails');
```

### Send topic

Send a message to all queues bound to patterns matching the given routing key.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$matched = Pgmq\sendTopic($pg, 'notifications.email', new Pgmq\SendMessage('{"user": 1}'));
```

### Send topic with delay

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$matched = Pgmq\sendTopic(
    $pg,
    'notifications.email',
    new Pgmq\SendMessage('{"user": 1}'),
    TimeSpan::fromSeconds(5),
);
```

### Test routing

Test which queues would receive a message for a given routing key without actually sending a message.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

foreach (Pgmq\testRouting($pg, 'notifications.email') as $route) {
    var_dump($route->pattern, $route->queue, $route->compiledRegex);
}
```

### Validate routing key

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

Pgmq\validateRoutingKey($pg, 'events.created'); // throws PostgresQueryError if invalid
```

### Validate topic pattern

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

Pgmq\validateTopicPattern($pg, 'events.*'); // throws PostgresQueryError if invalid
```

### Read grouped

Read messages respecting FIFO ordering within groups. Messages are grouped by the `x-pgmq-group` header. Only the oldest unprocessed message from each group is returned.

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
$queue->send(new Pgmq\SendMessage('{"action": "created"}', '{"x-pgmq-group": "order-1"}'));
$queue->send(new Pgmq\SendMessage('{"action": "paid"}', '{"x-pgmq-group": "order-1"}'));
$queue->send(new Pgmq\SendMessage('{"action": "created"}', '{"x-pgmq-group": "order-2"}'));

$messages = $queue->readGrouped(10, TimeSpan::fromSeconds(30));
```

### Read grouped round-robin

Read messages with round-robin distribution across groups.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
$messages = $queue->readGroupedRR(10);
```

### Read grouped head

Read exactly one message per FIFO group — the head (oldest, lowest msg_id) message in each group — across up to qty groups in a single operation.
Only groups with a visible, unlocked head message are included.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
$messages = $queue->readGroupedHead(10);
```

### Read grouped with poll

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
$messages = $queue->readGroupedWithPoll(
    count: 10,
    maxPoll: TimeSpan::fromSeconds(5),
    pollInterval: TimeSpan::fromMilliseconds(250),
);
```

### Read grouped round-robin with poll

```php
use Thesis\Pgmq;
use Amp\Postgres;
use Thesis\Time\TimeSpan;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
$messages = $queue->readGroupedRRWithPoll(
    count: 10,
    maxPoll: TimeSpan::fromSeconds(5),
    pollInterval: TimeSpan::fromMilliseconds(250),
);
```

### Create FIFO index

Create a GIN index on the headers column for FIFO queue performance optimization. This is required before using grouped read functions.

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

$queue = Pgmq\createQueue($pg, 'orders');
Pgmq\createFifoIndex($pg, 'orders');
```

### Create FIFO index for all queues

```php
use Thesis\Pgmq;
use Amp\Postgres;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString(''));

Pgmq\createFifoIndexAll($pg);
```

### Consume messages

This functionality is not a standard feature of the **pgmq** extension, but is provided by the library as an add-on for reliable and correct processing of message batches from the queue, with the ability to `ack`, `nack` (with delay) and archive (`term`) messages from the queue.

1. First of all, create the extension if it doesn't exist yet:

```php
use Thesis\Pgmq;

Pgmq\createExtension($pg);
```

2. Then create a queue:

```php
use Thesis\Pgmq;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');
```

3. Next, create the consumer object:

```php
use Thesis\Pgmq;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);
```

4. Now we can proceed to configure the queue consumer handler:

```php
use Thesis\Pgmq;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);

$context = $consumer->consume(
    static function (array $messages, Pgmq\ConsumeController $ctrl): void {
        var_dump($messages);
        $ctrl->ack($messages);
    },
    new Pgmq\ConsumeConfig(
        queue: 'events',
    ),
);
```

Through `Pgmq\ConsumeConfig` you can configure:

- the `batch` size of received messages;
- the message visibility timeout;
- enable monitoring for queue inserts via the LISTEN/NOTIFY mechanism;
- and set the polling interval.

At least one of these settings — `listenForInserts` or `pollTimeout` — must be specified.

Through the `Pgmq\ConsumeController`, you can:
- ack messages, causing them to be deleted from the queue;
- nack messages with a delay, setting a visibility timeout for them;
- terminate processing (when a message can no longer be retried), resulting in them being archived;
- stop the consumer.

Since receiving messages and `acking/nacking` them occur within the same transaction, for your own database queries you must use the `ConsumeController::$tx` object to ensure exactly-once semantics for message processing.

```php
use Thesis\Pgmq;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);

$context = $consumer->consume(
    static function (array $messages, Pgmq\ConsumeController $ctrl): void {
        $ctrl->tx->execute('...some business logic');
        $ctrl->ack($messages);
    },
    new Pgmq\ConsumeConfig(
        queue: 'events',
    ),
);
```

Using `ConsumeContext`, you can gracefully stop the consumer, waiting for the current batch to finish processing.


```php
use Thesis\Pgmq;
use function Amp\trapSignal;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);

$context = $consumer->consume(
    static function (array $messages, Pgmq\ConsumeController $ctrl): void {
        $ctrl->tx->execute('...some business logic');
        $ctrl->ack($messages);
    },
    new Pgmq\ConsumeConfig(
        queue: 'events',
    ),
);

trapSignal([\SIGINT, \SIGTERM])

$context->stop();
$context->awaitCompletion();
```

Or stop all current consumers using `$consumer->stop()`:

```php
use Thesis\Pgmq;
use function Amp\trapSignal;

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);

$context = $consumer->consume(...);

trapSignal([\SIGINT, \SIGTERM])

$consumer->stop();
$context->awaitCompletion();
```

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.

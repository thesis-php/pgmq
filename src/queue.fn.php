<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;
use Thesis\Time\TimeSpan;

/**
 * @api
 * @param non-empty-string $queue
 * @throws PostgresQueryError if queue name is invalid
 */
function validateQueueName(
    PostgresLink $pg,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.validate_queue_name(:queue_name);', [
        'queue_name' => $queue,
    ]);
}

/**
 * @api
 * @param non-empty-string $queue
 */
function createQueue(
    PostgresLink $pg,
    string $queue,
): Queue {
    $pg->execute('SELECT pgmq.create(:queue_name)', [
        'queue_name' => $queue,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @api
 * @param non-empty-string $queue
 */
function createUnloggedQueue(
    PostgresLink $pg,
    string $queue,
): Queue {
    $pg->execute('SELECT pgmq.create_unlogged(:queue_name)', [
        'queue_name' => $queue,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @param non-empty-string $queue
 * @throws QueueNotFound
 */
function findQueue(
    PostgresLink $pg,
    string $queue,
): Queue {
    $md = queueMetadata($pg, $queue);

    return new Queue($md->name, $pg);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param non-negative-int|non-empty-string $partitionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
 * @param non-negative-int|non-empty-string $retentionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
 */
function createPartitionedQueue(
    PostgresLink $pg,
    string $queue,
    int|string $partitionInterval = 10_000,
    int|string $retentionInterval = 100_000,
): Queue {
    $pg->execute('SELECT pgmq.create(:queue_name, :partition_interval, :retention_interval)', [
        'queue_name' => $queue,
        'partition_interval' => (string) $partitionInterval,
        'retention_interval' => (string) $retentionInterval,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @api
 * @return iterable<QueueMetadata>
 */
function listQueueMetadata(PostgresLink $pg): iterable
{
    $result = $pg->query('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues()');

    foreach ($result as $row) {
        yield QueueMetadata::fromArray($row);
    }
}

/**
 * @api
 * @return iterable<Queue>
 */
function listQueues(PostgresLink $pg): iterable
{
    $result = $pg->query('SELECT queue_name FROM pgmq.list_queues()');

    /** @var array{queue_name: non-empty-string} $row */
    foreach ($result as $row) {
        yield new Queue($row['queue_name'], $pg);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function dropQueue(
    PostgresLink $pg,
    string $queue,
): bool {
    /** @var array{drop_queue?: bool} $result */
    $result = $pg
        ->execute('SELECT pgmq.drop_queue(:queue_name)', [
            'queue_name' => $queue,
        ])
        ->fetchRow() ?? [];

    return $result['drop_queue'] ?? false;
}

/**
 * @api
 * @param non-empty-string $queue
 */
function purgeQueue(
    PostgresLink $pg,
    string $queue,
): int {
    /** @var array{purge_queue?: non-negative-int} $result */
    $result = $pg
        ->execute('SELECT pgmq.purge_queue(:queue_name)', [
            'queue_name' => $queue,
        ])
        ->fetchRow() ?? [];

    return $result['purge_queue'] ?? 0;
}

/**
 * @api
 * @param non-empty-string $queue
 * @throws QueueNotFound
 */
function queueMetrics(
    PostgresLink $pg,
    string $queue,
): QueueMetric {
    $result = $pg
        ->execute('SELECT * FROM pgmq.metrics(:queue_name)', [
            'queue_name' => $queue,
        ])
        ->fetchRow() ?? throw new QueueNotFound();

    return QueueMetric::fromArray($result);
}

/**
 * @api
 * @param non-empty-string $queue
 * @throws QueueNotFound
 */
function queueMetadata(
    PostgresLink $pg,
    string $queue,
): QueueMetadata {
    $result = $pg
        ->execute('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues() WHERE queue_name = :queue_name', [
            'queue_name' => $queue,
        ])
        ->fetchRow() ?? throw new QueueNotFound();

    return QueueMetadata::fromArray($result);
}

/**
 * @api
 * @return iterable<QueueMetric>
 */
function metrics(PostgresLink $pg): iterable
{
    $result = $pg->query('SELECT * FROM pgmq.metrics_all();');

    foreach ($result as $row) {
        yield QueueMetric::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @return int the message id, unique to the queue, is returned
 */
function send(
    PostgresLink $pg,
    string $queue,
    SendMessage $message,
    null|TimeSpan|\DateTimeImmutable $delay = null,
): int {
    $delay ??= TimeSpan::fromSeconds(0);

    $sql = match (true) {
        $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send(:queue_name, :msg, :headers, :delay::int)',
        default => 'SELECT * FROM pgmq.send(:queue_name, :msg, :headers, :delay::timestamptz)',
    };

    /** @var array{send: int} $result */
    $result = $pg
        ->execute($sql, [
            'queue_name' => $queue,
            'msg' => $message->valueJson,
            'headers' => $message->headerJson,
            'delay' => $delay instanceof TimeSpan ? $delay->toSeconds() : $delay->format(\DateTimeInterface::RFC3339),
        ])
        ->fetchRow() ?? throw new \RuntimeException("Failed to send message to the queue {$queue}.");

    return $result['send'];
}

/**
 * @api
 * @param non-empty-string $queue
 * @param non-empty-list<SendMessage> $messages
 * @return list<int>
 */
function sendBatch(
    PostgresLink $pg,
    string $queue,
    array $messages,
    null|TimeSpan|\DateTimeImmutable $delay = null,
): array {
    $delay ??= TimeSpan::fromSeconds(0);

    $sql = match (true) {
        $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send_batch(:queue_name, :msgs::jsonb[], :headers::jsonb[], :delay::int)',
        default => 'SELECT * FROM pgmq.send_batch(:queue_name, :msgs::jsonb[], :headers::jsonb[], :delay::timestamptz)',
    };

    $result = $pg->execute($sql, [
        'queue_name' => $queue,
        'msgs' => array_map(static fn(SendMessage $message): string => $message->valueJson, $messages),
        'headers' => array_map(static fn(SendMessage $message): ?string => $message->headerJson, $messages),
        'delay' => $delay instanceof TimeSpan ? $delay->toSeconds() : $delay->format(\DateTimeInterface::RFC3339),
    ]);

    $messageIds = [];

    /** @var array{send_batch: int} $row */
    foreach ($result as $row) {
        $messageIds[] = $row['send_batch'];
    }

    return $messageIds;
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $batch
 * @return iterable<Message>
 */
function readPoll(
    PostgresLink $pg,
    string $queue,
    int $batch = 1,
    ?TimeSpan $visibilityTimeout = null,
    ?TimeSpan $maxPoll = null,
    ?TimeSpan $pollInterval = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_with_poll(:queue_name, :vt, :limit, :poll_timeout_s, :poll_interval_ms);', [
        'queue_name' => $queue,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
        'limit' => $batch,
        'poll_timeout_s' => ($maxPoll ?? TimeSpan::fromSeconds(5))->toSeconds(),
        'poll_interval_ms' => ($pollInterval ?? TimeSpan::fromMilliseconds(250))->toMilliseconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function read(
    PostgresLink $pg,
    string $queue,
    ?TimeSpan $visibilityTimeout = null,
): ?Message {
    foreach (readBatch($pg, $queue, 1, $visibilityTimeout) as $message) {
        return $message;
    }

    return null;
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readBatch(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
): iterable {
    $visibilityTimeout ??= TimeSpan::fromSeconds(30);

    $result = $pg->execute('SELECT * FROM pgmq.read(:queue_name, :vt, :limit)', [
        'queue_name' => $queue,
        'vt' => $visibilityTimeout->toSeconds(),
        'limit' => $count,
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function pop(
    PostgresLink $pg,
    string $queue,
): ?Message {
    $row = $pg
        ->execute('SELECT * FROM pgmq.pop(:queue_name)', [
            'queue_name' => $queue,
        ])
        ->fetchRow();

    return $row !== null ? Message::fromArray($row) : null;
}

/**
 * @api
 * @param non-empty-string $queue
 */
function archive(
    PostgresLink $pg,
    string $queue,
    int $messageId,
): bool {
    return \in_array($messageId, archiveBatch($pg, $queue, [$messageId]), true);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param list<int> $messageIds
 * @return list<int>
 */
function archiveBatch(
    PostgresLink $pg,
    string $queue,
    array $messageIds,
): array {
    $result = $pg->execute('SELECT * FROM pgmq.archive(:queue_name, :msg_ids::bigint[])', [
        'queue_name' => $queue,
        'msg_ids' => $messageIds,
    ]);

    $archive = [];

    /** @var array{archive: int} $row */
    foreach ($result as $row) {
        $archive[] = $row['archive'];
    }

    return $archive;
}

/**
 * @api
 * @param non-empty-string $queue
 */
function delete(
    PostgresLink $pg,
    string $queue,
    int $messageId,
): bool {
    return \in_array($messageId, deleteBatch($pg, $queue, [$messageId]), true);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param list<int> $messageIds
 * @return list<int>
 */
function deleteBatch(
    PostgresLink $pg,
    string $queue,
    array $messageIds,
): array {
    $result = $pg->execute('SELECT pgmq.delete(:queue_name, :msg_ids::bigint[])', [
        'queue_name' => $queue,
        'msg_ids' => $messageIds,
    ]);

    $deleted = [];

    /** @var array{delete: int} $row */
    foreach ($result as $row) {
        $deleted[] = $row['delete'];
    }

    return $deleted;
}

/**
 * @api
 * @param non-empty-string $queue
 * @param list<int> $messageIds
 */
function setVisibilityTimeout(
    PostgresLink $pg,
    string $queue,
    array $messageIds,
    TimeSpan $visibilityTimeout,
): ?Message {
    $row = $pg
        ->execute('SELECT * FROM pgmq.set_vt(:queue_name, :msg_ids::bigint[], :vt::int)', [
            'queue_name' => $queue,
            'msg_ids' => $messageIds,
            'vt' => $visibilityTimeout->toSeconds(),
        ])
        ->fetchRow();

    return $row !== null ? Message::fromArray($row) : null;
}

/**
 * @api
 * @param non-empty-string $queue
 * @return non-empty-string
 */
function enableNotifyInsert(
    PostgresLink $pg,
    string $queue,
    ?TimeSpan $throttleInterval = null,
): string {
    $pg->execute('SELECT pgmq.enable_notify_insert(:queue_name, :throttle_interval_ms)', [
        'queue_name' => $queue,
        'throttle_interval_ms' => notifyThrottleMs($throttleInterval),
    ]);

    return channelName($queue);
}

/**
 * Makes insert notifications work for the queue, touching the trigger only when it is really needed:
 *
 * * already enabled with this throttle interval → nothing (catalog reads only, no lock on the queue table);
 * * enabled with another interval → `pgmq.update_notify_insert()` (no DDL);
 * * missing, disabled or calling another function, or the throttle row is gone → `pgmq.enable_notify_insert()`.
 *
 * The throttle row lives in an UNLOGGED table: crash recovery (or a promoted replica) empties it while the trigger
 * survives, and the trigger then never notifies. That is why the row is checked, not only the trigger.
 *
 * Check and change run as ONE statement under `pgmq.acquire_queue_lock()` (the lock pgmq itself takes to create and
 * drop the queue), so consumers starting together do not race into the DDL: the first one sets notifications up, the
 * others wait for it and then find them in place. Being one statement, it needs no transaction of its own and works
 * on any link: a pool, a connection or an open transaction (then the lock is held until that transaction ends).
 *
 * @api
 * @param non-empty-string $queue
 * @return non-empty-string
 */
function ensureNotifyInsert(
    PostgresLink $pg,
    string $queue,
    ?TimeSpan $throttleInterval = null,
): string {
    $pg->query(\sprintf(
        <<<'SQL'
            DO $ensure$
            DECLARE
                v_queue text := %s;
                v_throttle_ms integer := %d;
                v_current_ms integer;
            BEGIN
                PERFORM pgmq.acquire_queue_lock(v_queue);

                SELECT t.throttle_interval_ms INTO v_current_ms
                FROM pgmq.notify_insert_throttle t
                WHERE t.queue_name = v_queue;

                IF NOT (%s) OR v_current_ms IS NULL THEN
                    PERFORM pgmq.enable_notify_insert(v_queue, v_throttle_ms);
                ELSIF v_current_ms <> v_throttle_ms THEN
                    PERFORM pgmq.update_notify_insert(v_queue, v_throttle_ms);
                END IF;
            END
            $ensure$
            SQL,
        $pg->quoteLiteral($queue),
        notifyThrottleMs($throttleInterval),
        notifyTriggerValidSql('v_queue'),
    ));

    return channelName($queue);
}

/**
 * Whether insert notifications work for the queue with this throttle interval: the trigger exists, is enabled and
 * calls `pgmq.notify_queue_listeners()`, and the throttle row is in place. Reads the catalog only.
 *
 * @api
 * @param non-empty-string $queue
 */
function isNotifyInsertEnabled(
    PostgresLink $pg,
    string $queue,
    ?TimeSpan $throttleInterval = null,
): bool {
    /** @var array{enabled: bool} $row */
    $row = $pg
        ->execute(
            \sprintf(
                <<<'SQL'
                    SELECT (%s) AND EXISTS (
                        SELECT 1
                        FROM pgmq.notify_insert_throttle t
                        WHERE t.queue_name = :queue_name AND t.throttle_interval_ms = :throttle_interval_ms
                    ) AS enabled
                    SQL,
                notifyTriggerValidSql(':queue_name'),
            ),
            [
                'queue_name' => $queue,
                'throttle_interval_ms' => notifyThrottleMs($throttleInterval),
            ],
        )
        ->fetchRow();

    return $row['enabled'];
}

/**
 * SQL condition "the queue has a working notify trigger": it exists, is enabled and calls the pgmq function.
 * One definition for {@see ensureNotifyInsert()} and {@see isNotifyInsertEnabled()}.
 *
 * @internal
 * @param non-empty-string $queueExpression SQL expression with the queue name (a parameter or a PL/pgSQL variable)
 */
function notifyTriggerValidSql(string $queueExpression): string
{
    return \sprintf(
        <<<'SQL'
            EXISTS (
                SELECT 1
                FROM pg_trigger tg
                JOIN pg_class c ON c.oid = tg.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'pgmq'
                  AND c.relname = pgmq.format_table_name(%s, 'q')
                  AND tg.tgname = 'trigger_notify_queue_insert_listeners'
                  AND tg.tgenabled IN ('O', 'A')
                  AND tg.tgfoid = 'pgmq.notify_queue_listeners()'::regprocedure
            )
            SQL,
        $queueExpression,
    );
}

/**
 * @internal
 */
function notifyThrottleMs(?TimeSpan $throttleInterval): int
{
    return ($throttleInterval ?? TimeSpan::fromMilliseconds(30))->toMilliseconds();
}

/**
 * @api
 * @param non-empty-string $queue
 */
function disableNotifyInsert(
    PostgresLink $pg,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.disable_notify_insert(:queue_name)', [
        'queue_name' => $queue,
    ]);
}

/**
 * @api
 * @param non-empty-string $queue
 * @return non-empty-string
 */
function channelName(string $queue): string
{
    return "pgmq.q_{$queue}.INSERT";
}

/**
 * @api
 */
function createConsumer(
    PostgresConnection $pg,
): Consumer {
    return new Consumer($pg);
}

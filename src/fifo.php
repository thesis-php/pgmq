<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;
use Thesis\Time\TimeSpan;

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readGroupedRR(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_grouped_rr(:queue_name, :vt, :qty)', [
        'queue_name' => $queue,
        'qty' => $count,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readGroupedRRWithPoll(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
    ?TimeSpan $maxPoll = null,
    ?TimeSpan $pollInterval = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_grouped_rr_with_poll(:queue_name, :vt, :qty, :max_poll_seconds, :poll_interval_ms)', [
        'queue_name' => $queue,
        'qty' => $count,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
        'max_poll_seconds' => ($maxPoll ?? TimeSpan::fromSeconds(5))->toSeconds(),
        'poll_interval_ms' => ($pollInterval ?? TimeSpan::fromMilliseconds(250))->toMilliseconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readGrouped(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_grouped(:queue_name, :vt, :qty)', [
        'queue_name' => $queue,
        'qty' => $count,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readGroupedWithPoll(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
    ?TimeSpan $maxPoll = null,
    ?TimeSpan $pollInterval = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_grouped_with_poll(:queue_name, :vt, :qty, :max_poll_seconds, :poll_interval_ms)', [
        'queue_name' => $queue,
        'qty' => $count,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
        'max_poll_seconds' => ($maxPoll ?? TimeSpan::fromSeconds(5))->toSeconds(),
        'poll_interval_ms' => ($pollInterval ?? TimeSpan::fromMilliseconds(250))->toMilliseconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<int, Message>
 */
function readGroupedHead(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_grouped_head(:queue_name, :vt, :qty)', [
        'queue_name' => $queue,
        'qty' => $count,
        'vt' => ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function createFifoIndex(
    PostgresLink $pg,
    string $queue,
): void {
    $pg->execute('SELECT * FROM pgmq.create_fifo_index(:queue_name)', [
        'queue_name' => $queue,
    ]);
}

/**
 * @api
 */
function createFifoIndexAll(PostgresLink $pg): void
{
    $pg->execute('SELECT * FROM pgmq.create_fifo_indexes_all()');
}

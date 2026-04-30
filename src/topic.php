<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;
use Thesis\Time\TimeSpan;

/**
 * @api
 * @param non-empty-string $pattern
 * @param non-empty-string $queue
 */
function bindTopic(
    PostgresLink $pg,
    string $pattern,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.bind_topic(:pattern, :queue_name);', [
        'pattern' => $pattern,
        'queue_name' => $queue,
    ]);
}

/**
 * @api
 * @param non-empty-string $pattern
 * @param non-empty-string $queue
 */
function unbindTopic(
    PostgresLink $pg,
    string $pattern,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.unbind_topic(:pattern, :queue_name);', [
        'pattern' => $pattern,
        'queue_name' => $queue,
    ]);
}

/**
 * @api
 * @param non-empty-string $routingKey
 * @return non-negative-int
 */
function sendTopic(
    PostgresLink $pg,
    string $routingKey,
    SendMessage $message,
    null|TimeSpan|\DateTimeImmutable $delay = null,
): int {
    $delay ??= TimeSpan::fromSeconds(0);

    if ($delay instanceof \DateTimeImmutable) {
        $delay = TimeSpan::diff($delay, new \DateTimeImmutable());
    }

    /** @var array{send_topic: non-negative-int} $result */
    $result = $pg
        ->execute('SELECT * FROM pgmq.send_topic(:routing_key, :msg, :headers, :delay::int)', [
            'routing_key' => $routingKey,
            'msg' => $message->valueJson,
            'headers' => $message->headerJson,
            'delay' => max((int) $delay->toSeconds(), 0),
        ])
        ->fetchRow() ?? throw new \RuntimeException("Failed to send message using routingKey {$routingKey}.");

    return $result['send_topic'];
}

/**
 * @api
 * @param non-empty-string $routingKey
 * @return iterable<int, TopicRoute>
 */
function testRouting(
    PostgresLink $pg,
    string $routingKey,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.test_routing(:routing_key)', [
        'routing_key' => $routingKey,
    ]);

    foreach ($result as $row) {
        yield TopicRoute::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $routingKey
 * @throws PostgresQueryError if routingKey is invalid
 */
function validateRoutingKey(
    PostgresLink $pg,
    string $routingKey,
): void {
    $pg->execute('SELECT * FROM pgmq.validate_routing_key(:routing_key)', [
        'routing_key' => $routingKey,
    ]);
}

/**
 * @api
 * @param non-empty-string $pattern
 * @throws PostgresQueryError if topic pattern is invalid
 */
function validateTopicPattern(
    PostgresLink $pg,
    string $pattern,
): void {
    $pg->execute('SELECT * FROM pgmq.validate_topic_pattern(:pattern)', [
        'pattern' => $pattern,
    ]);
}

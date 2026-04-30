<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres;
use Thesis\Pgmq;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString('host=pgmq user=postgres password=postgres'));

Pgmq\createExtension($pg);

$queue = Pgmq\createQueue($pg, 'orders');
$queue->createFifoIndex();

// Send messages with group headers for FIFO ordering
$queue->send(new Pgmq\SendMessage('{"order": 1, "action": "created"}', '{"x-pgmq-group": "customer-1"}'));
$queue->send(new Pgmq\SendMessage('{"order": 1, "action": "paid"}', '{"x-pgmq-group": "customer-1"}'));
$queue->send(new Pgmq\SendMessage('{"order": 2, "action": "created"}', '{"x-pgmq-group": "customer-2"}'));
$queue->send(new Pgmq\SendMessage('{"order": 2, "action": "paid"}', '{"x-pgmq-group": "customer-2"}'));

// Read one message per group (FIFO within each group)
$messages = [...$queue->readGroupedRR(10)];

dump(array_map(static fn(Pgmq\Message $m): string => $m->value, $messages));

$queue->deleteBatch(array_map(static fn(Pgmq\Message $message): int => $message->id, $messages));

$queue->drop();

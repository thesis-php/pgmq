<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresTransaction;
use Thesis\Pgmq;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class ConsumeController
{
    /**
     * @param non-empty-string $queue
     */
    public function __construct(
        public PostgresTransaction $tx,
        private string $queue,
        private ConsumeContext $context,
    ) {}

    public function stop(): void
    {
        $this->context->stop();
    }

    /**
     * @param non-empty-list<Message> $messages
     */
    public function ack(array $messages): void
    {
        Pgmq\deleteBatch(
            pg: $this->tx,
            queue: $this->queue,
            messageIds: array_map(static fn(Message $m) => $m->id, $messages),
        );
    }

    /**
     * @param non-empty-list<Message> $messages
     */
    public function nack(array $messages, TimeSpan $delay): void
    {
        Pgmq\setVisibilityTimeout(
            pg: $this->tx,
            queue: $this->queue,
            messageIds: array_map(static fn(Message $m) => $m->id, $messages),
            visibilityTimeout: $delay,
        );
    }

    /**
     * @param non-empty-list<Message> $messages
     */
    public function term(array $messages): void
    {
        Pgmq\archiveBatch(
            pg: $this->tx,
            queue: $this->queue,
            messageIds: array_map(static fn(Message $m) => $m->id, $messages),
        );
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresTransaction;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class ConsumeController
{
    public function __construct(
        public PostgresTransaction $tx,
        private Queue $queue,
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
        $this->queue->deleteBatch(array_map(
            static fn(Message $message): int => $message->id,
            $messages,
        ));
    }

    /**
     * @param non-empty-list<Message> $messages
     */
    public function nack(array $messages, TimeSpan $delay): void
    {
        $this->queue->setVisibilityTimeout(
            array_map(
                static fn(Message $message): int => $message->id,
                $messages,
            ),
            $delay,
        );
    }

    /**
     * @param non-empty-list<Message> $messages
     */
    public function term(array $messages): void
    {
        $this->queue->archiveBatch(array_map(
            static fn(Message $message): int => $message->id,
            $messages,
        ));
    }
}

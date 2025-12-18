<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class Queue
{
    /**
     * @param non-empty-string $name
     */
    public function __construct(
        public string $name,
        private PostgresLink $pg,
    ) {}

    public function drop(): bool
    {
        return dropQueue(
            pg: $this->pg,
            queue: $this->name,
        );
    }

    public function purge(): int
    {
        return purgeQueue(
            pg: $this->pg,
            queue: $this->name,
        );
    }

    /**
     * @throws QueueNotFound
     */
    public function metrics(): QueueMetric
    {
        return queueMetrics(
            pg: $this->pg,
            queue: $this->name,
        );
    }

    /**
     * @throws QueueNotFound
     */
    public function metadata(): QueueMetadata
    {
        return queueMetadata(
            pg: $this->pg,
            queue: $this->name,
        );
    }

    /**
     * @return int the message id, unique to the queue, is returned
     */
    public function send(SendMessage $message, null|TimeSpan|\DateTimeImmutable $delay = null): int
    {
        return send(
            pg: $this->pg,
            queue: $this->name,
            message: $message,
            delay: $delay,
        );
    }

    /**
     * @param non-empty-list<SendMessage> $messages
     * @return list<int>
     */
    public function sendBatch(array $messages, null|TimeSpan|\DateTimeImmutable $delay = null): array
    {
        return sendBatch(
            pg: $this->pg,
            queue: $this->name,
            messages: $messages,
            delay: $delay,
        );
    }

    /**
     * @param positive-int $batch
     * @return iterable<Message>
     */
    public function readPoll(
        int $batch = 1,
        ?TimeSpan $visibilityTimeout = null,
        ?TimeSpan $maxPoll = null,
        ?TimeSpan $pollInterval = null,
    ): iterable {
        return readPoll(
            pg: $this->pg,
            queue: $this->name,
            batch: $batch,
            visibilityTimeout: $visibilityTimeout,
            maxPoll: $maxPoll,
            pollInterval: $pollInterval,
        );
    }

    public function read(?TimeSpan $visibilityTimeout = null): ?Message
    {
        return read(
            pg: $this->pg,
            queue: $this->name,
            visibilityTimeout: $visibilityTimeout,
        );
    }

    /**
     * @param positive-int $count
     * @return iterable<Message>
     */
    public function readBatch(int $count, ?TimeSpan $visibilityTimeout = null): iterable
    {
        return readBatch(
            pg: $this->pg,
            queue: $this->name,
            count: $count,
            visibilityTimeout: $visibilityTimeout,
        );
    }

    public function pop(): ?Message
    {
        return pop(
            pg: $this->pg,
            queue: $this->name,
        );
    }

    public function archive(int $messageId): bool
    {
        return archive(
            pg: $this->pg,
            queue: $this->name,
            messageId: $messageId,
        );
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function archiveBatch(array $messageIds): array
    {
        return archiveBatch(
            pg: $this->pg,
            queue: $this->name,
            messageIds: $messageIds,
        );
    }

    public function delete(int $messageId): bool
    {
        return delete(
            pg: $this->pg,
            queue: $this->name,
            messageId: $messageId,
        );
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function deleteBatch(array $messageIds): array
    {
        return deleteBatch(
            pg: $this->pg,
            queue: $this->name,
            messageIds: $messageIds,
        );
    }

    /**
     * @param list<int> $messageIds
     */
    public function setVisibilityTimeout(array $messageIds, TimeSpan $visibilityTimeout): ?Message
    {
        return setVisibilityTimeout(
            pg: $this->pg,
            queue: $this->name,
            messageIds: $messageIds,
            visibilityTimeout: $visibilityTimeout,
        );
    }

    /**
     * @return non-empty-string
     */
    public function enableNotifyInsert(?TimeSpan $throttleInterval = null): string
    {
        return enableNotifyInsert(
            pg: $this->pg,
            queue: $this->name,
            throttleInterval: $throttleInterval,
        );
    }

    public function disableNotifyInsert(): void
    {
        disableNotifyInsert(
            pg: $this->pg,
            queue: $this->name,
        );
    }
}

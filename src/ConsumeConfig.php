<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class ConsumeConfig
{
    private const int DEFAULT_BATCH = 10;
    private const int DEFAULT_POLL_INTERVAL_SECS = 3;

    public TimeSpan $pollInterval;

    /**
     * @param non-empty-string $queue
     * @param positive-int $batch
     * @param bool $listenForInserts will call {@see ensureNotifyInsert} and will listen for notifications from the channel, which significantly optimizes the number of requests to the Postgres server for new messages. It is recommended to enable it.
     */
    public function __construct(
        public string $queue,
        public int $batch = self::DEFAULT_BATCH,
        ?TimeSpan $pollInterval = null,
        public ?TimeSpan $visibilityTimeout = null,
        public bool $listenForInserts = true,
    ) {
        $this->pollInterval = $pollInterval ?? TimeSpan::fromSeconds(self::DEFAULT_POLL_INTERVAL_SECS);
    }
}

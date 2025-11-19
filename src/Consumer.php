<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Future;
use Amp\Postgres\PostgresConnection;
use function Amp\async;

/**
 * @api
 */
final class Consumer
{
    /** @var list<Internal\ConsumeHandler> */
    private array $consumers = [];

    public function __construct(
        private readonly PostgresConnection $pg,
    ) {}

    /**
     * @param callable(non-empty-list<Message>, ConsumeController): void $handler
     * @throws QueueNotFound
     */
    public function consume(
        callable $handler,
        ConsumeConfig $config,
    ): ConsumeContext {
        $queue = findQueue($this->pg, $config->queue);

        $polls = new Internal\PollQueue();

        /** @var list<Internal\PollWatcher> $watchers */
        $watchers = [];

        $timeoutWatcher = $config->pollInterval->isPositive()
            ? new Internal\TimeoutWatcher($polls, $config->pollInterval)
            : null;

        if ($timeoutWatcher !== null) {
            $watchers[] = $timeoutWatcher;
        }

        if ($config->listenForInserts) {
            $channelName = $queue->enableNotifyInsert();

            $watchers[] = new Internal\ChannelWatcher(
                $polls,
                $this->pg->listen($channelName),
                $timeoutWatcher,
            );
        }

        if (\count($watchers) === 0) {
            throw new \LogicException('At least one watcher must be configured. Either set a positive $pollInterval or enable $listenForInserts or both.');
        }

        $handle = new Internal\ConsumeHandler(
            $this->pg,
            $config,
            $handler,
            new Internal\AggregateWatcher($watchers),
            $polls,
        );

        $this->consumers[] = $handle;

        return $handle->context;
    }

    public function stop(): void
    {
        $futures = [];

        $consumers = $this->consumers;
        $this->consumers = [];

        foreach ($consumers as $consumer) {
            $futures[] = async($consumer->stop(...));
        }

        Future\awaitAll($futures);
    }
}

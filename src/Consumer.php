<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Future;
use Amp\Pipeline;
use Amp\Postgres\PostgresConnection;
use function Amp\async;
use function Thesis\Pgmq\Internal\consume;

/**
 * @api
 */
final class Consumer
{
    /** @var list<ConsumeContext> */
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

        /** @var Pipeline\Queue<null> $polls */
        $polls = new Pipeline\Queue(1);

        // Initial poll request.
        $polls->push(null);

        /** @var list<Internal\PollWatcher> $watchers */
        $watchers = [];

        $timeoutWatcher = $config->pollInterval->isPositive()
            ? new Internal\TimeoutWatcher($polls, $config->pollInterval)
            : throw new \LogicException('Pooling is required. Set $pollInterval to a positive value.');

        $watchers[] = $timeoutWatcher;

        if ($config->listenForInserts) {
            $channelName = $queue->enableNotifyInsert();

            $watchers[] = new Internal\ChannelWatcher(
                $polls,
                $this->pg->listen($channelName),
                $timeoutWatcher,
            );
        }

        $context = consume(
            $this->pg,
            $config,
            $handler,
            new Internal\AggregateWatcher($watchers),
            $polls,
        );

        return $this->consumers[] = $context;
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

<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\Postgres\PostgresListener;
use Revolt\EventLoop;

/**
 * @internal
 */
final readonly class ChannelWatcher implements PollWatcher
{
    public function __construct(
        private PollQueue $queue,
        private PostgresListener $listener,
        private ?TimeoutWatcher $timeout = null,
    ) {}

    public function watch(): void
    {
        EventLoop::queue(function (): void {
            foreach ($this->listener as $_) {
                $this->timeout?->reschedule();
                $this->queue->push(null);
            }
        });
    }

    public function cancel(): void
    {
        $this->listener->unlisten();
    }
}

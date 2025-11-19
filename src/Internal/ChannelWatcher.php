<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\Pipeline;
use Amp\Postgres\PostgresListener;
use Revolt\EventLoop;

/**
 * @internal
 */
final readonly class ChannelWatcher implements PollWatcher
{
    /**
     * @param Pipeline\Queue<null> $queue
     */
    public function __construct(
        private Pipeline\Queue $queue,
        private PostgresListener $listener,
        private ?TimeoutWatcher $timeout = null,
    ) {}

    public function watch(): void
    {
        EventLoop::queue(function (): void {
            foreach ($this->listener as $_) {
                if (!$this->queue->isComplete()) {
                    $this->timeout?->reschedule();
                    $this->queue->push(null);
                }
            }
        });
    }

    public function cancel(): void
    {
        $this->listener->unlisten();
    }
}

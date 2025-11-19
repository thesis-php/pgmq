<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\Pipeline;
use Revolt\EventLoop;
use Thesis\Time\TimeSpan;

/**
 * @internal
 */
final class TimeoutWatcher implements PollWatcher
{
    private ?string $callbackId = null;

    /**
     * @param Pipeline\Queue<null> $queue
     */
    public function __construct(
        private readonly Pipeline\Queue $queue,
        private readonly TimeSpan $interval,
    ) {}

    public function reschedule(): void
    {
        $this->cancel();
        $this->watch();
    }

    public function watch(): void
    {
        $this->callbackId = EventLoop::repeat($this->interval->toSeconds(), function (): void {
            if (!$this->queue->isComplete()) {
                $this->queue->push(null);
            }
        });
    }

    public function cancel(): void
    {
        if ($this->callbackId !== null) {
            $callbackId = $this->callbackId;
            $this->callbackId = null;

            EventLoop::cancel($callbackId);
        }
    }
}

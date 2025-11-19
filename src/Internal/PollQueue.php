<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\Pipeline;

/**
 * @api
 */
final readonly class PollQueue
{
    /** @var Pipeline\ConcurrentIterator<null> */
    private Pipeline\ConcurrentIterator $iterator;

    /** @var Pipeline\Queue<null> */
    private Pipeline\Queue $queue;

    public function __construct()
    {
        /** @var Pipeline\Queue<null> $queue */
        $queue = new Pipeline\Queue(1);
        $queue->push(null);

        $this->queue = $queue;
        $this->iterator = $queue->iterate();
    }

    public function completed(): bool
    {
        return $this->queue->isComplete();
    }

    public function dispose(): void
    {
        $this->iterator->dispose();
    }

    public function complete(): void
    {
        if (!$this->queue->isComplete()) {
            $this->queue->complete();
        }
    }

    public function push(null $value): void
    {
        $this->queue->push($value);
    }

    public function pop(): bool
    {
        if ($this->iterator->continue()) {
            $this->iterator->getValue();

            return true;
        }

        return false;
    }
}

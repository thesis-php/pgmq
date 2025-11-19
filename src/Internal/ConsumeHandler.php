<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\DeferredFuture;
use Amp\Postgres\PostgresConnection;
use Revolt\EventLoop;
use Thesis\Pgmq;

/**
 * @internal
 */
final readonly class ConsumeHandler
{
    public Pgmq\ConsumeContext $context;

    /** @var DeferredFuture<*> */
    private DeferredFuture $completionMarker;

    /**
     * @param callable(non-empty-list<Pgmq\Message>, Pgmq\ConsumeController): void $handler
     */
    public function __construct(
        PostgresConnection $pg,
        Pgmq\ConsumeConfig $config,
        callable $handler,
        PollWatcher $watcher,
        private PollQueue $polls,
    ) {
        $this->completionMarker = $completionMarker = new DeferredFuture();

        $this->context = $context = new Pgmq\ConsumeContext(
            $stop = $this->stop(...),
            $this->completionMarker->getFuture(),
        );

        EventLoop::queue(static function () use (
            $pg,
            $config,
            $handler,
            $polls,
            $watcher,
            $completionMarker,
            $context,
            $stop,
        ): void {
            $watcher->watch();

            while (!$polls->completed()) {
                if (!$polls->pop()) {
                    break;
                }

                $tx = $pg->beginTransaction();

                try {
                    $messages = Pgmq\readBatch(
                        pg: $tx,
                        queue: $config->queue,
                        count: $config->batch,
                        visibilityTimeout: $config->visibilityTimeout,
                    );

                    $messages = [...$messages];

                    if (\count($messages) > 0) {
                        $handler(
                            $messages,
                            new Pgmq\ConsumeController($tx, new Pgmq\Queue($config->queue, $tx), $context),
                        );
                    }

                    $tx->commit();
                } catch (\Throwable $e) {
                    $tx->rollback();
                    $completionMarker->error($e);
                    $stop();
                }
            }

            $watcher->cancel();

            if (!$completionMarker->isComplete()) {
                $completionMarker->complete();
            }
        });
    }

    public function stop(): void
    {
        $this->polls->complete();
    }
}

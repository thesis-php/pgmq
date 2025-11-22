<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\DeferredFuture;
use Amp\Pipeline;
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
     * @param Pipeline\Queue<null> $polls
     */
    public function __construct(
        PostgresConnection $pg,
        Pgmq\ConsumeConfig $config,
        callable $handler,
        PollWatcher $watcher,
        private Pipeline\Queue $polls,
    ) {
        $iterator = $polls->iterate();
        $this->completionMarker = $completionMarker = new DeferredFuture();

        $this->context = $context = new Pgmq\ConsumeContext(
            $this->stop(...),
            $this->completionMarker->getFuture(),
        );

        EventLoop::queue(static function () use (
            $pg,
            $config,
            $handler,
            $iterator,
            $watcher,
            $completionMarker,
            $context,
        ): void {
            $watcher->watch();

            foreach ($iterator as $_) {
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
                    $iterator->dispose();
                    break;
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
        if (!$this->polls->isComplete()) {
            $this->polls->complete();
        }
    }
}

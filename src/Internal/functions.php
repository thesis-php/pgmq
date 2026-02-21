<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\DeferredFuture;
use Amp\Pipeline;
use Amp\Postgres\PostgresConnection;
use Revolt\EventLoop;
use Thesis\Pgmq;

/**
 * @param callable(non-empty-list<Pgmq\Message>, Pgmq\ConsumeController): void $handler
 * @param Pipeline\Queue<null> $polls
 */
function consume(
    PostgresConnection $pg,
    Pgmq\ConsumeConfig $config,
    callable $handler,
    PollWatcher $watcher,
    Pipeline\Queue $polls,
): Pgmq\ConsumeContext {
    $iterator = $polls->iterate();
    $completionMarker = new DeferredFuture();

    $context = new Pgmq\ConsumeContext(
        stop: static function () use ($watcher, $polls): void {
            if (!$polls->isComplete()) {
                $watcher->cancel();
                $polls->complete();
            }
        },
        completionMarker: $completionMarker->getFuture(),
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
                        new Pgmq\ConsumeController($tx, $config->queue, $context),
                    );
                }

                $tx->commit();
            } catch (\Throwable $e) {
                $tx->rollback();
                $completionMarker->error($e);
                $watcher->cancel();
                $iterator->dispose();
                break;
            }
        }

        if (!$completionMarker->isComplete()) {
            $completionMarker->complete();
        }
    });

    return $context;
}

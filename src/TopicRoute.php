<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 * @phpstan-type RawRoute = array{
 *      pattern: non-empty-string,
 *      queue_name: non-empty-string,
 *      compiled_regex: non-empty-string,
 *  }
 */
final readonly class TopicRoute
{
    /**
     * @internal
     * @param array<array-key, mixed> $row
     */
    public static function fromArray(array $row): self
    {
        /** @var RawRoute $row */
        return new self(
            pattern: $row['pattern'],
            queue: $row['queue_name'],
            compiledRegex: $row['compiled_regex'],
        );
    }

    /**
     * @param non-empty-string $pattern
     * @param non-empty-string $queue
     * @param non-empty-string $compiledRegex
     */
    public function __construct(
        public string $pattern,
        public string $queue,
        public string $compiledRegex,
    ) {}
}

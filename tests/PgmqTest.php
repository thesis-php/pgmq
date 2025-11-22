<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresQueryError;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Thesis\Time\TimeSpan;
use function Amp\delay;

#[CoversClass(Queue::class)]
final class PgmqTest extends TestCase
{
    private const string TESTING_MESSAGE = '{"ping": "pong"}';
    private const string TESTING_HEADERS = '{"x": "y"}';

    private PostgresConnection $pg;

    protected function setUp(): void
    {
        parent::setUp();

        $dsn = getenv('THESIS_PGMQ_DSN');

        if (!\is_string($dsn) || $dsn === '') {
            self::markTestSkipped('Set the THESIS_PGMQ_DSN environment variable.');
        }

        $this->pg = new PostgresConnectionPool(PostgresConfig::fromString($dsn));
        createExtension($this->pg);

        foreach (listQueues($this->pg) as $queue) {
            $queue->drop();
        }
    }

    public function testValidateQueueName(): void
    {
        validateQueueName($this->pg, $this->randomQueueName());

        self::expectException(PostgresQueryError::class);
        self::expectExceptionMessage('queue name is too long, maximum length is 47 characters');

        validateQueueName($this->pg, $this->randomQueueName() . $this->randomQueueName());
    }

    public function testCreateQueue(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $metrics = $queue->metrics();
        self::assertSame($queue->name, $metrics->name);
        self::assertSame(0, $metrics->totalMessages);
        self::assertSame(0, $metrics->length);
        self::assertSame(0, $metrics->queueVisibleLength);
        self::assertTrue($metrics->newestMsgAge->isZero());
        self::assertTrue($metrics->oldestMsgAge->isZero());

        $metadata = $queue->metadata();

        self::assertSame($queue->name, $metadata->name);
        self::assertFalse($metadata->partitioned);
        self::assertFalse($metadata->unlogged);

        $queue->drop();

        self::expectException(QueueNotFound::class);
        $queue->metadata();
    }

    public function testCreateUnloggedQueue(): void
    {
        $queue = createUnloggedQueue($this->pg, $this->randomQueueName());

        $metadata = $queue->metadata();

        self::assertSame($queue->name, $metadata->name);
        self::assertFalse($metadata->partitioned);
        self::assertTrue($metadata->unlogged);

        $queue->drop();
    }

    public function testMetricsAll(): void
    {
        createQueue($this->pg, $this->randomQueueName());
        createQueue($this->pg, $this->randomQueueName());

        foreach (metrics($this->pg) as $metrics) {
            self::assertSame(0, $metrics->totalMessages);
            self::assertSame(0, $metrics->length);
            self::assertSame(0, $metrics->queueVisibleLength);
            self::assertTrue($metrics->newestMsgAge->isZero());
            self::assertTrue($metrics->oldestMsgAge->isZero());
        }
    }

    public function testSendAndReadMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS));

        $message = $queue->read(TimeSpan::fromSeconds(20));
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
        self::assertSame(self::TESTING_HEADERS, $message->headers);
    }

    public function testSendAndReadDelayedMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE), delay: $delay = TimeSpan::fromSeconds(1));

        self::assertNull($queue->read());

        delay($delay->add(TimeSpan::fromMilliseconds(50))->toSeconds());

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testSendAndReadDelayedWithTimestampMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE), delay: new \DateTimeImmutable('+1 seconds'));

        self::assertNull($queue->read());

        delay(1.05);

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testArchiveMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);

        $queue->archive($message->id);

        self::assertNull($queue->read());
    }

    public function testDeleteMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);

        $queue->delete($message->id);

        self::assertNull($queue->read());
    }

    public function testSendAndReadBatch(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS),
            new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS),
        ]);
        self::assertCount(2, $messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(2, $messages);

        /** @var Message $message */
        foreach ($messages as $message) {
            self::assertSame(self::TESTING_MESSAGE, $message->value);
            self::assertSame(self::TESTING_HEADERS, $message->headers);
        }

        self::assertCount(0, [...$queue->readBatch(2)]);
    }

    public function testPopMessage(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        $message = $queue->pop();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testReadPoll(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $queue->send(new SendMessage(self::TESTING_MESSAGE));

        $messages = [...$queue->readPoll()];
        self::assertCount(1, $messages);
        self::assertSame(self::TESTING_MESSAGE, $messages[0]->value);
    }

    public function testArchiveBatch(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);
        $queue->archiveBatch($messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(0, $messages);
    }

    public function testDeleteBatch(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);
        $queue->deleteBatch($messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(0, $messages);
    }

    public function testPurgeQueue(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $queue->send(new SendMessage(self::TESTING_MESSAGE));

        self::assertSame(1, $queue->metrics()->length);

        self::assertSame(1, $queue->purge());

        self::assertSame(0, $queue->metrics()->length);
    }

    public function testEnableDisableNotifies(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        self::assertSame(channelName($queue->name), $queue->enableNotifyInsert());

        $queue->disableNotifyInsert();
    }

    public function testInvalidConsumerConfiguration(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $consumer = createConsumer($this->pg);

        self::expectException(\LogicException::class);
        self::expectExceptionMessage('At least one watcher must be configured. Either set a positive $pollInterval or enable $listenForInserts or both.');
        $consumer->consume(static fn() => null, new ConsumeConfig(
            queue: $queue->name,
            pollInterval: TimeSpan::fromSeconds(0),
            listenForInserts: false,
        ));
    }

    public function testConsumeBatch(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);

        self::assertCount(2, $messageIds);
        self::assertSame(2, $queue->metrics()->length);

        /** @var array<non-negative-int, non-empty-string> $consumed */
        $consumed = [];

        $consumer = createConsumer($this->pg);
        $context = $consumer->consume(
            static function (array $messages, ConsumeController $ctrl) use (&$consumed): void {
                /** @var Message $message */
                foreach ($messages as $message) {
                    $consumed[$message->id] = $message->value;
                }

                $ctrl->ack($messages);
                $ctrl->stop();
            },
            new ConsumeConfig($queue->name),
        );

        $context->awaitCompletion();

        self::assertCount(2, $consumed);
        self::assertEquals($messageIds, array_keys($consumed));
        self::assertEquals([self::TESTING_MESSAGE, self::TESTING_MESSAGE], array_values($consumed));
        self::assertSame(0, $queue->metrics()->length);
    }

    public function testStopConsumeOnUnhandledException(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $consumer = createConsumer($this->pg);

        $context = $consumer->consume(
            handler: static function (): void {
                throw new \RuntimeException('Exception from consumer');
            },
            config: new ConsumeConfig(
                queue: $queue->name,
            ),
        );

        delay(0.1);

        send($this->pg, $queue->name, new SendMessage(self::TESTING_MESSAGE));
        send($this->pg, $queue->name, new SendMessage(self::TESTING_MESSAGE));

        self::expectException(\RuntimeException::class);
        self::expectExceptionMessage('Exception from consumer');
        $context->awaitCompletion();
    }

    /**
     * @return non-empty-string
     */
    private function randomQueueName(): string
    {
        /** @var non-empty-string */
        return substr(bin2hex(random_bytes(30)), 0, length: 30);
    }
}

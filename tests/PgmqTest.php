<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresQueryError;
use Amp\TimeoutCancellation;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DoesNotPerformAssertions;
use PHPUnit\Framework\TestCase;
use Thesis\Time\TimeSpan;
use function Amp\async;
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

        $this->expectException(PostgresQueryError::class);
        $this->expectExceptionMessage('queue name is too long, maximum length is 47 characters');

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

        $this->expectException(QueueNotFound::class);
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
        /** @phpstan-ignore offsetAccess.notFound */
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

        self::assertSame(channelName($queue->name), $queue->enableNotifyInsert(TimeSpan::fromSeconds(1)));

        $queue->disableNotifyInsert();
    }

    public function testConsumerStartDoesNotWaitForProducerTransactions(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->enableNotifyInsert();

        // A producer is in the middle of a transaction that has already inserted into the queue.
        $producer = $this->pg->beginTransaction();
        send($producer, $queue->name, new SendMessage(self::TESTING_MESSAGE));

        $pg = $this->pg;

        try {
            // A worker (re)starts: notifications are already set up, so it must not touch the trigger. Recreating it
            // takes a table lock that waits for every open producer transaction and blocks new inserts meanwhile.
            /** @var ConsumeContext $context */
            $context = async(static fn(): ConsumeContext => createConsumer($pg)->consume(
                static function (): void {},
                new ConsumeConfig($queue->name),
            ))->await(new TimeoutCancellation(1));

            $context->stop();
            $context->awaitCompletion();
        } finally {
            $producer->rollback();
        }

        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testEnsureNotifyInsertKeepsExistingSetupAndEnablesMissingOne(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        self::assertFalse($queue->isNotifyInsertEnabled());
        self::assertSame(channelName($queue->name), $queue->ensureNotifyInsert());
        self::assertTrue($queue->isNotifyInsertEnabled());

        // Already enabled with the same throttle: nothing to do.
        self::assertSame(channelName($queue->name), $queue->ensureNotifyInsert());
        self::assertTrue($queue->isNotifyInsertEnabled());

        // A different throttle is a different setup: it is applied.
        $queue->ensureNotifyInsert(TimeSpan::fromSeconds(1));
        self::assertTrue($queue->isNotifyInsertEnabled(TimeSpan::fromSeconds(1)));
        self::assertFalse($queue->isNotifyInsertEnabled());

        $queue->disableNotifyInsert();
        self::assertFalse($queue->isNotifyInsertEnabled(TimeSpan::fromSeconds(1)));
    }

    public function testConcurrentColdStartsCreateTheTriggerOnce(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName()); // notifications not set up yet

        // A producer keeps an insert into the queue open, so that every consumer start has to wait for the table lock
        // and the starts are guaranteed to overlap.
        $producer = $this->pg->beginTransaction();
        send($producer, $queue->name, new SendMessage(self::TESTING_MESSAGE));

        $dsn = (string) getenv('THESIS_PGMQ_DSN');
        $creations = $this->countTriggerCreations(static function () use ($dsn, $queue, $producer): void {
            $starts = [];

            for ($i = 0; $i < 4; ++$i) {
                // Each consumer has its own pool, as separate worker processes do (one pool cannot LISTEN twice).
                $pool = new PostgresConnectionPool(PostgresConfig::fromString($dsn));
                $starts[] = async(static fn(): ConsumeContext => createConsumer($pool)->consume(
                    static function (): void {},
                    new ConsumeConfig($queue->name),
                ));
            }

            delay(0.5);
            $producer->rollback();

            foreach ($starts as $start) {
                /** @var ConsumeContext $context */
                $context = $start->await(new TimeoutCancellation(5));
                $context->stop();
                $context->awaitCompletion();
            }
        }, $queue->name);

        self::assertSame(1, $creations, 'concurrent starts must not race into enable_notify_insert()');
        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testEnsureNotifyInsertRestoresLostThrottleRow(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->enableNotifyInsert();

        // pgmq.notify_insert_throttle is UNLOGGED: crash recovery empties it while the trigger survives.
        $this->pg->execute('DELETE FROM pgmq.notify_insert_throttle WHERE queue_name = :queue_name', ['queue_name' => $queue->name]);
        self::assertFalse($queue->isNotifyInsertEnabled());

        $queue->ensureNotifyInsert();

        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testEnsureNotifyInsertRecreatesDisabledTrigger(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->enableNotifyInsert();
        $this->pg->query(\sprintf('ALTER TABLE pgmq.q_%s DISABLE TRIGGER trigger_notify_queue_insert_listeners', $queue->name));
        self::assertFalse($queue->isNotifyInsertEnabled());

        $queue->ensureNotifyInsert();

        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testEnsureNotifyInsertRecreatesTriggerCallingAnotherFunction(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->enableNotifyInsert();
        $this->pg->query('CREATE OR REPLACE FUNCTION public.thesis_test_noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$');
        $this->pg->query(\sprintf('DROP TRIGGER trigger_notify_queue_insert_listeners ON pgmq.q_%s', $queue->name));
        $this->pg->query(\sprintf(
            'CREATE CONSTRAINT TRIGGER trigger_notify_queue_insert_listeners AFTER INSERT ON pgmq.q_%s DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE public.thesis_test_noop_trigger()',
            $queue->name,
        ));
        self::assertFalse($queue->isNotifyInsertEnabled());

        $queue->ensureNotifyInsert();

        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testEnsureNotifyInsertWorksInsideAnOpenTransaction(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        $tx = $this->pg->beginTransaction();
        ensureNotifyInsert($tx, $queue->name);
        self::assertTrue(isNotifyInsertEnabled($tx, $queue->name));
        $tx->rollback();

        self::assertFalse($queue->isNotifyInsertEnabled(), 'the setup belongs to the caller\'s transaction');
    }

    public function testEnsureNotifyInsertChangesThrottleWithoutRecreatingTrigger(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->enableNotifyInsert(TimeSpan::fromSeconds(1));

        $creations = $this->countTriggerCreations(static fn() => $queue->ensureNotifyInsert(), $queue->name);

        self::assertSame(0, $creations);
        self::assertTrue($queue->isNotifyInsertEnabled());
    }

    public function testInvalidConsumerConfiguration(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $consumer = createConsumer($this->pg);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('Pooling is required. Set $pollInterval to a positive value.');
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

    public function testNackBatch(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);

        self::assertCount(2, $messageIds);
        self::assertSame(2, $queue->metrics()->length);

        $count = 0;

        /** @var array<non-negative-int, list<non-empty-string>> $consumed */
        $consumed = [];

        $consumer = createConsumer($this->pg);
        $context = $consumer->consume(
            static function (array $messages, ConsumeController $ctrl) use (&$consumed, &$count): void {
                /** @var Message $message */
                foreach ($messages as $message) {
                    $consumed[$message->id][] = $message->value;
                    ++$count;
                }

                if ($count === 2) {
                    $ctrl->nack($messages, TimeSpan::fromSeconds(1));
                } elseif ($count > 2) {
                    $ctrl->ack($messages);
                    $ctrl->stop();
                }
            },
            new ConsumeConfig($queue->name, pollInterval: TimeSpan::fromMilliseconds(500)),
        );

        $context->awaitCompletion();

        self::assertCount(2, $consumed);
        self::assertEquals($messageIds, array_keys($consumed));
        self::assertEquals([[self::TESTING_MESSAGE, self::TESTING_MESSAGE], [self::TESTING_MESSAGE, self::TESTING_MESSAGE]], array_values($consumed));
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

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Exception from consumer');
        $context->awaitCompletion();
    }

    #[DoesNotPerformAssertions]
    public function testConsumerStoppingShouldNotCompeteWithPolling(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $consumer = createConsumer($this->pg);

        $config = new ConsumeConfig(
            queue: $queue->name,
            batch: 1,
            pollInterval: TimeSpan::fromMilliseconds(1),
            listenForInserts: false,
        );

        $ctx = $consumer->consume(
            static function (array $messages, ConsumeController $c): void {},
            $config,
        );

        $ctx->stop();
        $ctx->awaitCompletion();
    }

    public function testBindAndSendTopic(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        bindTopic($this->pg, 'events.*', $queue->name);

        $messageId = sendTopic($this->pg, 'events.created', new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS));

        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
        self::assertSame(self::TESTING_HEADERS, $message->headers);
    }

    public function testSendTopicWithDelay(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        bindTopic($this->pg, 'orders.*', $queue->name);

        sendTopic($this->pg, 'orders.placed', new SendMessage(self::TESTING_MESSAGE), TimeSpan::fromSeconds(1));

        self::assertNull($queue->read());

        delay(1.05);

        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testSendTopicToMultipleQueues(): void
    {
        $queue1 = createQueue($this->pg, $this->randomQueueName());
        $queue2 = createQueue($this->pg, $this->randomQueueName());

        bindTopic($this->pg, 'notifications.*', $queue1->name);
        bindTopic($this->pg, 'notifications.*', $queue2->name);

        $messages = sendTopic($this->pg, 'notifications.email', new SendMessage(self::TESTING_MESSAGE));

        self::assertSame(2, $messages);
        $message1 = $queue1->read();
        self::assertNotNull($message1);
        self::assertSame(self::TESTING_MESSAGE, $message1->value);

        $message2 = $queue2->read();
        self::assertNotNull($message2);
        self::assertSame(self::TESTING_MESSAGE, $message2->value);
    }

    public function testUnbindTopic(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());

        bindTopic($this->pg, 'events.*', $queue->name);

        unbindTopic($this->pg, 'events.*', $queue->name);

        sendTopic($this->pg, 'events.created', new SendMessage(self::TESTING_MESSAGE));

        self::assertNull($queue->read());
    }

    public function testTestRouting(): void
    {
        $queue1 = createQueue($this->pg, $this->randomQueueName());
        $queue2 = createQueue($this->pg, $this->randomQueueName());

        bindTopic($this->pg, 'events.*', $queue1->name);
        bindTopic($this->pg, 'events.created', $queue2->name);

        $routes = [...testRouting($this->pg, 'events.created')];

        self::assertCount(2, $routes);

        $queueNames = array_map(static fn(TopicRoute $route): string => $route->queue, $routes);
        self::assertContains($queue1->name, $queueNames);
        self::assertContains($queue2->name, $queueNames);

        foreach ($routes as $route) {
            self::assertNotEmpty($route->pattern);
            self::assertNotEmpty($route->compiledRegex);
        }
    }

    public function testReadGrouped(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();

        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "b"}'));

        $messages = [...$queue->readGrouped(10)];

        self::assertCount(3, $messages);

        $groups = array_map(
            static fn(Message $message) => self::findHeader($message, 'x-pgmq-group', \strval(...)),
            $messages,
        );

        self::assertContains('a', $groups);
        self::assertContains('b', $groups);
    }

    public function testReadGroupedRR(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();

        $queue->send(new SendMessage('{"order": 1}', '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage('{"order": 2}', '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage('{"order": 3}', '{"x-pgmq-group": "b"}'));
        $queue->send(new SendMessage('{"order": 4}', '{"x-pgmq-group": "b"}'));

        $messages = [...$queue->readGroupedRR(10)];

        self::assertCount(4, $messages);

        $groups = array_map(
            static fn(Message $message) => self::findHeader($message, 'x-pgmq-group', \strval(...)),
            $messages,
        );

        self::assertContains('a', $groups);
        self::assertContains('b', $groups);
    }

    public function testReadGroupedHead(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();

        $queue->send(new SendMessage('{"order": 1}', '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage('{"order": 2}', '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage('{"order": 3}', '{"x-pgmq-group": "b"}'));

        $messages = [...$queue->readGroupedHead(10)];

        self::assertCount(2, $messages);

        $groups = array_map(
            static fn(Message $message) => self::findHeader($message, 'x-pgmq-group', \strval(...)),
            $messages,
        );

        self::assertContains('a', $groups);
        self::assertContains('b', $groups);
    }

    public function testReadGroupedWithPoll(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();

        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "b"}'));

        $messages = [...$queue->readGroupedWithPoll(
            count: 10,
            maxPoll: TimeSpan::fromSeconds(1),
            pollInterval: TimeSpan::fromMilliseconds(100),
        )];

        self::assertCount(2, $messages);
    }

    public function testReadGroupedRRWithPoll(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();

        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "a"}'));
        $queue->send(new SendMessage(self::TESTING_MESSAGE, '{"x-pgmq-group": "b"}'));

        $messages = [...$queue->readGroupedRRWithPoll(
            count: 10,
            maxPoll: TimeSpan::fromSeconds(1),
            pollInterval: TimeSpan::fromMilliseconds(100),
        )];

        self::assertCount(2, $messages);
    }

    #[DoesNotPerformAssertions]
    public function testCreateFifoIndex(): void
    {
        $queue = createQueue($this->pg, $this->randomQueueName());
        $queue->createFifoIndex();
    }

    #[DoesNotPerformAssertions]
    public function testCreateFifoIndexAll(): void
    {
        createQueue($this->pg, $this->randomQueueName());
        createQueue($this->pg, $this->randomQueueName());

        createFifoIndexAll($this->pg);
    }

    public function testValidateRoutingKey(): void
    {
        validateRoutingKey($this->pg, 'events.created');

        $this->expectException(PostgresQueryError::class);

        validateRoutingKey($this->pg, 'events.*');
    }

    public function testValidateTopicPattern(): void
    {
        validateTopicPattern($this->pg, 'events.*');

        $this->expectException(PostgresQueryError::class);

        validateTopicPattern($this->pg, 'logs.**');
    }

    /**
     * Runs $action and returns how many times the notify trigger of the queue was created meanwhile (event trigger).
     *
     * @param callable(): mixed $action
     * @param non-empty-string $queue
     */
    private function countTriggerCreations(callable $action, string $queue): int
    {
        $this->pg->query('CREATE TABLE IF NOT EXISTS public.thesis_test_ddl (object_identity text)');
        $this->pg->query('TRUNCATE public.thesis_test_ddl');
        $this->pg->query(<<<'SQL'
            CREATE OR REPLACE FUNCTION public.thesis_test_log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
            DECLARE r record;
            BEGIN
                FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                    INSERT INTO public.thesis_test_ddl (object_identity) VALUES (r.object_identity);
                END LOOP;
            END $$
            SQL);
        $this->pg->query('DROP EVENT TRIGGER IF EXISTS thesis_test_ddl');
        $this->pg->query("CREATE EVENT TRIGGER thesis_test_ddl ON ddl_command_end WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION public.thesis_test_log_ddl()");

        try {
            $action();
        } finally {
            $this->pg->query('DROP EVENT TRIGGER IF EXISTS thesis_test_ddl');
        }

        /** @var array{n: int} $row */
        $row = $this->pg
            ->execute('SELECT count(*)::int AS n FROM public.thesis_test_ddl WHERE object_identity LIKE :pattern', ['pattern' => '% on pgmq.q_' . $queue])
            ->fetchRow();

        return $row['n'];
    }

    /**
     * @return non-empty-string
     */
    private function randomQueueName(): string
    {
        /** @var non-empty-string */
        return substr(bin2hex(random_bytes(30)), 0, length: 30);
    }

    /**
     * @template T
     * @param non-empty-string $header
     * @param callable(scalar): T $coerce
     * @param T $default
     * @return ($default is null ? (?T) : T)
     */
    private static function findHeader(
        Message $message,
        string $header,
        callable $coerce,
        mixed $default = null,
    ): mixed {
        /** @var array<non-empty-string, scalar> $headers */
        $headers = json_decode($message->headers ?? '{}', true);

        if (isset($headers[$header])) {
            return $coerce($headers[$header]);
        }

        return $default;
    }
}

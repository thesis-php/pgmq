<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres;
use Thesis\Pgmq;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString('host=pgmq user=postgres password=postgres'));

Pgmq\createExtension($pg);

$emailQueue = Pgmq\createQueue($pg, 'emails');
$smsQueue = Pgmq\createQueue($pg, 'sms');

Pgmq\bindTopic($pg, 'notifications.*', $emailQueue->name);
Pgmq\bindTopic($pg, 'notifications.urgent', $smsQueue->name);

// Route to all queues matching the pattern
Pgmq\sendTopic($pg, 'notifications.urgent', new Pgmq\SendMessage('{"user": 1, "text": "Server is down"}'));

dump($emailQueue->pop());
dump($smsQueue->pop());

// Route only to email queue
Pgmq\sendTopic($pg, 'notifications.welcome', new Pgmq\SendMessage('{"user": 2, "text": "Welcome!"}'));

dump($emailQueue->pop());
dump($smsQueue->pop());

Pgmq\unbindTopic($pg, 'notifications.*', $emailQueue->name);
Pgmq\unbindTopic($pg, 'notifications.urgent', $smsQueue->name);

$emailQueue->drop();
$smsQueue->drop();

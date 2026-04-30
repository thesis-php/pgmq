<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;

/**
 * @api
 */
function createExtension(PostgresLink $pg): void
{
    $pg->execute('CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;');
}

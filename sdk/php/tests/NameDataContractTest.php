<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Connection;
use Bunqueue\Job;
use Bunqueue\Wire\Protocol;
use PHPUnit\Framework\TestCase;

final class NameDataContractTest extends TestCase
{
  public function testNamedPayloadKeepsUserDataAndPrimitivesSeparate(): void
  {
    self::assertSame(
      ['name' => 'send-email', 'data' => ['name' => 'customer-visible', 'to' => 'a@b.c']],
      Protocol::jobPayload('send-email', ['name' => 'customer-visible', 'to' => 'a@b.c']),
    );
    self::assertSame(
      ['name' => 'scalar-op', 'data' => 42],
      Protocol::jobPayload('scalar-op', 42),
    );
  }

  public function testJobPrefersTopLevelNameAndOnlyUnwrapsLegacyData(): void
  {
    $connection = new Connection();
    $modern = new Job([
      'name' => 'modern-op',
      'data' => ['name' => 'user-name', 'value' => 1],
    ], $connection);
    $legacy = new Job(['data' => ['name' => 'legacy-op', 'value' => 2]], $connection);
    $scalar = new Job(['name' => 'scalar-op', 'data' => false], $connection);

    self::assertSame('modern-op', $modern->name());
    self::assertSame(['name' => 'user-name', 'value' => 1], $modern->data());
    self::assertSame('legacy-op', $legacy->name());
    self::assertSame(['value' => 2], $legacy->data());
    self::assertFalse($scalar->data());
  }
}

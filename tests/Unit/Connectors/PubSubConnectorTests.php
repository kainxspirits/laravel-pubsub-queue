<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Kainxspirits\PubSubQueue\Connectors\PubSubConnector;
use Kainxspirits\PubSubQueue\PubSubQueue;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class PubSubConnectorTests extends TestCase
{
    public function testImplementsConnectorInterface(): void
    {
        putenv('SUPPRESS_GCLOUD_CREDS_WARNING=true');
        $reflection = new ReflectionClass(PubSubConnector::class);
        $this->assertTrue($reflection->implementsInterface(ConnectorInterface::class));
    }

    public function testConnectReturnsPubSubQueueInstance(): void
    {
        $connector = new PubSubConnector;
        $config = $this->createFakeConfig();
        $queue = $connector->connect($config);

        $this->assertTrue($queue instanceof PubSubQueue);
        $this->assertEquals($queue->getSubscriberName(), 'test-subscriber');
    }

    public function testQueuePrefixAdded(): void
    {
        $connector = new PubSubConnector();
        $config = $this->createFakeConfig() + ['queue_prefix' => 'prefix-'];
        $queue = $connector->connect($config);

        $this->assertEquals('prefix-my-queue', $queue->getQueue('my-queue'));
    }

    public function testNotQueuePrefixAddedMultipleTimes(): void
    {
        $connector = new PubSubConnector();
        $config = $this->createFakeConfig() + ['queue_prefix' => 'prefix-'];
        $queue = $connector->connect($config);

        $this->assertEquals('prefix-default', $queue->getQueue($queue->getQueue('default')));
    }

    private function createFakeConfig()
    {
        return [
            'queue' => 'test',
            'project_id' => 'the-project-id',
            'subscriber' => 'test-subscriber',
            'retries' => 1,
            'request_timeout' => 60,
        ];
    }
}

<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit\Connectors;

use ReflectionClass;
use PHPUnit\Framework\TestCase;
use Kainxspirits\PubSubQueue\PubSubQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Kainxspirits\PubSubQueue\Connectors\PubSubConnector;

class PubSubConnectorTests extends TestCase
{
    public function testImplementsConnectorInterface()
    {
        putenv('SUPPRESS_GCLOUD_CREDS_WARNING=true');
        $reflection = new ReflectionClass(PubSubConnector::class);
        $this->assertTrue($reflection->implementsInterface(ConnectorInterface::class));
    }

    public function testConnectReturnsPubSubQueueInstance()
    {
        $connector = new PubSubConnector;
        $config = $this->createFakeConfig();
        $queue = $connector->connect($config);

        $this->assertTrue($queue instanceof PubSubQueue);
    }

    private function createFakeConfig()
    {
        return [
            'queue' => 'test',
            'project_id' => 'the-project-id',
            'retries' => 1,
            'request_timeout' => 60,
        ];
    }
}

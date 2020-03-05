<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit\Jobs;

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Kainxspirits\PubSubQueue\Jobs\PubSubJob;
use Kainxspirits\PubSubQueue\PubSubQueue;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class PubSubJobTests extends TestCase
{
    public function setUp(): void
    {
        $this->messageId = '1234';
        $this->messageData = json_encode(['id' => $this->messageId, 'foo' => 'bar']);
        $this->messageEncodedData = base64_encode($this->messageData);

        $this->container = $this->createMock(Container::class);
        $this->queue = $this->createMock(PubSubQueue::class);
        $this->client = $this->createMock(PubSubClient::class);

        $this->message = $this->getMockBuilder(Message::class)
            ->setConstructorArgs([[], []])
            ->setMethods(['data', 'id', 'attributes'])
            ->getMock();

        $this->message->method('data')
            ->willReturn($this->messageEncodedData);

        $this->message->method('id')
            ->willReturn($this->messageId);

        $this->message->method('attributes')
            ->with($this->equalTo('attempts'))
            ->willReturn(42);

        $this->job = $this->getMockBuilder(PubSubJob::class)
            ->setConstructorArgs([$this->container, $this->queue, $this->message, 'test', 'test'])
            ->setMethods()
            ->getMock();
    }

    public function testImplementsJobInterface()
    {
        $reflection = new ReflectionClass(PubSubJob::class);
        $this->assertTrue($reflection->implementsInterface(JobContract::class));
    }

    public function testGetJobId()
    {
        $this->assertEquals($this->job->getJobId(), $this->messageId);
    }

    public function testGetRawBody()
    {
        $this->assertEquals($this->job->getRawBody(), $this->messageData);
    }

    public function testDeleteMethodSetDeletedProperty()
    {
        $this->job->delete();
        $this->assertTrue($this->job->isDeleted());
    }

    public function testAttempts()
    {
        $this->assertTrue(is_int($this->job->attempts()));
    }

    public function testReleaseAndPublish()
    {
        $this->queue->expects($this->once())
            ->method('republish')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->callback(function ($options) {
                    if (! is_array($options)) {
                        return false;
                    }

                    foreach ($options as $key => $option) {
                        if (! is_string($option) || ! is_string($key)) {
                            return false;
                        }
                    }

                    return true;
                })
            );

        $this->job->release();
    }

    public function testReleaseMethodSetReleasedProperty()
    {
        $this->job->release();
        $this->assertTrue($this->job->isReleased());
    }
}

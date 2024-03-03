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

final class PubSubJobTests extends TestCase
{
    /**
     * @var string
     */
    protected $messageId;

    /**
     * @var string
     */
    protected $messageData;

    /**
     * @var string
     */
    protected $messageEncodedData;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&Container
     */
    protected $container;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue
     */
    protected $queue;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&PubSubClient
     */
    protected $client;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&Message
     */
    protected $message;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&PubSubJob
     */
    protected $job;

    protected function setUp(): void
    {
        $this->messageId = '1234';
        $this->messageData = json_encode(['id' => $this->messageId, 'foo' => 'bar']);
        $this->messageEncodedData = base64_encode($this->messageData);

        $this->container = $this->createMock(Container::class);
        $this->queue = $this->createMock(PubSubQueue::class);
        $this->client = $this->createMock(PubSubClient::class);

        $this->message = $this->getMockBuilder(Message::class)
            ->setConstructorArgs([[], []])
            ->onlyMethods(['data', 'id', 'attributes'])
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
            ->onlyMethods([])
            ->getMock();
    }

    public function testImplementsJobInterface(): void
    {
        $reflection = new ReflectionClass(PubSubJob::class);
        $this->assertTrue($reflection->implementsInterface(JobContract::class));
    }

    public function testGetJobId(): void
    {
        $this->assertEquals($this->job->getJobId(), $this->messageId);
    }

    public function testGetRawBody(): void
    {
        $this->assertEquals($this->job->getRawBody(), $this->messageData);
    }

    public function testDeleteMethodSetDeletedProperty(): void
    {
        $this->job->delete();
        $this->assertTrue($this->job->isDeleted());
    }

    public function testAttempts(): void
    {
        $this->assertTrue(is_int($this->job->attempts()));
    }

    public function testReleaseAndPublish(): void
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

    public function testReleaseMethodSetReleasedProperty(): void
    {
        $this->job->release();
        $this->assertTrue($this->job->isReleased());
    }
}

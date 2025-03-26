<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit;

use Carbon\Carbon;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Kainxspirits\PubSubQueue\Jobs\PubSubJob;
use Kainxspirits\PubSubQueue\PubSubQueue;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class PubSubQueueTests extends TestCase
{
    /**
     * @var string
     */
    protected $expectedResult = 'message-id';

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&Topic
     */
    protected $topic;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&PubSubClient
     */
    protected $client;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&Subscription
     */
    protected $subscription;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&Message
     */
    protected $message;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue
     */
    protected $queue;

    protected function setUp(): void
    {
        $this->expectedResult = 'message-id';

        $this->topic = $this->createMock(Topic::class);
        $this->client = $this->createMock(PubSubClient::class);
        $this->subscription = $this->createMock(Subscription::class);
        $this->message = $this->createMock(Message::class);

        $this->queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods([
                'pushRaw',
                'getTopic',
                'availableAt',
                'subscribeToTopic',
            ])->getMock();
    }

    public function testImplementsQueueInterface(): void
    {
        $reflection = new ReflectionClass(PubSubQueue::class);
        $this->assertTrue($reflection->implementsInterface(QueueContract::class));
    }

    public function testPushNewJob(): void
    {
        $job = 'test';
        $data = ['foo' => 'bar'];

        $this->queue->setContainer(Container::getInstance());

        $this->queue->expects($this->once())
            ->method('pushRaw')
            ->willReturn($this->expectedResult)
            ->with($this->callback(function ($payload) use ($job, $data) {
                $decoded_payload = json_decode($payload, true);

                return $decoded_payload['data'] === $data && $decoded_payload['job'] === $job;
            }));

        $this->assertEquals($this->expectedResult, $this->queue->push('test', $data));
    }

    public function testPushRaw(): void
    {
        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods(['getTopic', 'subscribeToTopic'])
            ->getMock();

        $payload = json_encode(['id' => $this->expectedResult]);

        $this->topic->method('publish')
            ->willReturn($this->expectedResult)
            ->with($this->callback(function ($publish) use ($payload) {
                $decoded_payload = $publish['data'];

                return $decoded_payload === $payload;
            }));

        $queue->method('getTopic')
            ->willReturn($this->topic);

        $queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $this->assertEquals($this->expectedResult, $queue->pushRaw($payload));
    }

    public function testPushRawOptionsOnlyAcceptKeyValueStrings(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods(['getTopic', 'subscribeToTopic'])
            ->getMock();

        $this->topic->method('publish')
            ->willReturn($this->expectedResult);

        $queue->method('getTopic')
            ->willReturn($this->topic);

        $queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $payload = json_encode(['id' => $this->expectedResult]);

        $options = [
            'integer' => 42,
            'array' => [
                'foo' => 'bar',
            ],
            1 => 'wrong key',
            'object' => new \stdClass,
        ];

        $queue->pushRaw($payload, '', $options);
    }

    public function testLater(): void
    {
        $job = 'test';
        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->queue->expects($this->once())
            ->method('pushRaw')
            ->willReturn($this->expectedResult)
            ->with(
                $this->isType('string'),
                $this->anything(),
                $this->callback(function ($options) use ($delay_timestamp) {
                    if (! is_array($options)) {
                        return false;
                    }

                    foreach ($options as $key => $option) {
                        if (! is_string($option) || ! is_string($key)) {
                            return false;
                        }
                    }

                    if (! isset($options['available_at']) || $options['available_at'] !== (string) $delay_timestamp) {
                        return false;
                    }

                    return true;
                })
            );

        $this->assertEquals($this->expectedResult, $this->queue->later($delay, $job, ['foo' => 'bar']));
    }

    public function testPopWhenJobsAvailable(): void
    {
        $this->subscription->expects($this->once())
            ->method('acknowledge');

        $this->subscription->method('pull')
            ->willReturn([$this->message]);

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('exists')
            ->willReturn(true);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->message->method('data')
            ->willReturn(json_encode(['foo' => 'bar']));

        $this->queue->setContainer($this->createMock(Container::class));

        $this->assertTrue($this->queue->pop('test') instanceof PubSubJob);
    }

    public function testPopWhenNoJobAvailable(): void
    {
        $this->subscription->expects($this->exactly(0))
            ->method('acknowledge');

        $this->subscription->method('pull')
            ->willReturn([]);

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('exists')
            ->willReturn(true);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->assertTrue(is_null($this->queue->pop('test')));
    }

    public function testPopWhenTopicDoesNotExist(): void
    {
        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->topic->method('exists')
            ->willReturn(false);

        $this->assertTrue(is_null($this->queue->pop('test')));
    }

    public function testPopWhenJobDelayed(): void
    {
        $delay = 60;
        $timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $message = $this->message->method('attribute')
            ->willReturn($timestamp);

        $this->subscription->method('pull')
            ->willReturn([$this->message]);

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('exists')
            ->willReturn(true);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->setContainer($this->createMock(Container::class));

        $this->assertTrue(is_null($this->queue->pop('test')));
    }

    public function testBulk(): void
    {
        $jobs = ['test'];
        $data = ['foo' => 'bar'];

        $this->topic->expects($this->once())
            ->method('publishBatch')
            ->willReturn($this->expectedResult)
            ->with($this->callback(function ($payloads) use ($jobs, $data) {
                $decoded_payload = json_decode($payloads[0]['data'], true);

                return $decoded_payload['job'] === $jobs[0] && $decoded_payload['data'] === $data;
            }));

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $this->assertEquals($this->expectedResult, $this->queue->bulk($jobs, $data));
    }

    public function testAcknowledge(): void
    {
        $this->subscription->expects($this->once())
            ->method('acknowledge');

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->acknowledge($this->message);
    }

    public function testRepublish(): void
    {
        $options = ['foo' => 'bar'];
        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->topic->expects($this->once())
            ->method('publish')
            ->willReturn($this->expectedResult)
            ->with(
                $this->callback(function ($message) use ($options, $delay_timestamp) {
                    if (! isset($message['attributes']) || ! is_array($message['attributes'])) {
                        return false;
                    }

                    foreach ($message['attributes'] as $key => $attribute) {
                        if (! is_string($attribute) || ! is_string($key)) {
                            return false;
                        }
                    }

                    if (! isset($message['attributes']['available_at']) || $message['attributes']['available_at'] !== (string) $delay_timestamp) {
                        return false;
                    }

                    if (! isset($message['attributes']['foo']) || $message['attributes']['foo'] != $options['foo']) {
                        return false;
                    }

                    return true;
                }),
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

        $this->queue->republish($this->message, 'test', $options, $delay);
    }

    public function testRepublishOptionsOnlyAcceptString(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->topic->method('publish')
            ->willReturn($this->expectedResult);

        $options = [
            'integer' => 42,
            'array' => [
                'foo' => 'bar',
            ],
            1 => 'wrong key',
            'object' => new \stdClass,
        ];

        $this->queue->republish($this->message, 'test', $options, $delay);
    }

    public function testGetTopic(): void
    {
        $this->topic->method('exists')
            ->willReturn(true);

        $this->client->method('topic')
            ->willReturn($this->topic);

        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods([])
            ->getMock();

        $this->assertTrue($queue->getTopic('test') instanceof Topic);
    }

    public function testCreateTopicAndReturnIt(): void
    {
        $this->topic->method('exists')
            ->willReturn(false);

        $this->topic->expects($this->once())
            ->method('create')
            ->willReturn(true);

        $this->client->method('topic')
            ->willReturn($this->topic);

        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods([])
            ->getMock();

        $this->assertTrue($queue->getTopic('test', true) instanceof Topic);
    }

    public function testSubscribtionIsCreated(): void
    {
        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('subscribe')
            ->willReturn($this->subscription);

        $this->subscription->method('exists')
            ->willReturn(false);

        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods([])
            ->getMock();

        $this->assertTrue($queue->subscribeToTopic($this->topic) instanceof Subscription);
    }

    public function testSubscriptionIsRetrieved(): void
    {
        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->subscription->method('exists')
            ->willReturn(true);

        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->onlyMethods([])
            ->getMock();

        $this->assertTrue($queue->subscribeToTopic($this->topic) instanceof Subscription);
    }

    public function testGetSubscriberName(): void
    {
        /** @var \PHPUnit\Framework\MockObject\MockObject&PubSubQueue $queue */
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', 'test-subscriber'])
            ->onlyMethods([])
            ->getMock();

        $this->assertTrue(is_string($queue->getSubscriberName()));
        $this->assertEquals($queue->getSubscriberName(), 'test-subscriber');
    }

    public function testGetPubSub(): void
    {
        $this->assertTrue($this->queue->getPubSub() instanceof PubSubClient);
    }
}

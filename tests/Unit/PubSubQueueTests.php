<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit;

use Carbon\Carbon;
use ReflectionClass;
use Google\Cloud\PubSub\Topic;
use PHPUnit\Framework\TestCase;
use Google\Cloud\PubSub\Message;
use Illuminate\Container\Container;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Kainxspirits\PubSubQueue\PubSubQueue;
use Kainxspirits\PubSubQueue\Jobs\PubSubJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class PubSubQueueTests extends TestCase
{
    public function teardown()
    {
        //
    }

    public function setUp()
    {
        $this->result = 'message-id';

        $this->topic = $this->createMock(Topic::class);
        $this->client = $this->createMock(PubSubClient::class);
        $this->subscription = $this->createMock(Subscription::class);
        $this->message = $this->createMock(Message::class);

        $this->queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods([
                'pushRaw',
                'getTopic',
                'exists',
                'subscription',
                'availableAt',
                'subscribeToTopic',
            ])->getMock();
    }

    public function testImplementsQueueInterface()
    {
        $reflection = new ReflectionClass(PubSubQueue::class);
        $this->assertTrue($reflection->implementsInterface(QueueContract::class));
    }

    public function testPushNewJob()
    {
        $job = 'test';
        $data = ['foo' => 'bar'];

        $this->queue->expects($this->once())
            ->method('pushRaw')
            ->willReturn($this->result)
            ->with($this->callback(function ($payload) use ($job, $data) {
                $decoded_payload = json_decode($payload, true);

                return $decoded_payload['data'] === $data && $decoded_payload['job'] === $job;
            }));

        $this->assertEquals($this->result, $this->queue->push('test', $data));
    }

    public function testPushRaw()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods(['getTopic', 'subscribeToTopic'])
            ->getMock();

        $payload = json_encode(['id' => $this->result]);

        $this->topic->method('publish')
            ->willReturn($this->result)
            ->with($this->callback(function ($publish) use ($payload) {
                $decoded_payload = base64_decode($publish['data']);

                return $decoded_payload === $payload;
            }));

        $queue->method('getTopic')
            ->willReturn($this->topic);

        $queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $this->assertEquals($this->result, $queue->pushRaw($payload));
    }

    public function testPushRawOptionsOnlyAcceptKeyValueStrings()
    {
        $this->expectException(\UnexpectedValueException::class);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods(['getTopic', 'subscribeToTopic'])
            ->getMock();

        $this->topic->method('publish')
            ->willReturn($this->result);

        $queue->method('getTopic')
            ->willReturn($this->topic);

        $queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $payload = json_encode(['id' => $this->result]);

        $options = [
            'integer' => 42,
            'array' => [
                'foo' => 'bar',
            ],
            1 => 'wrong key',
            'object' => new \StdClass,
        ];

        $queue->pushRaw($payload, '', $options);
    }

    public function testLater()
    {
        $job = 'test';
        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->queue->expects($this->once())
            ->method('pushRaw')
            ->willReturn($this->result)
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

        $this->assertEquals($this->result, $this->queue->later($delay, $job, ['foo' => 'bar']));
    }

    public function testPopWhenJobsAvailable()
    {
        $this->subscription->method('pull')
            ->willReturn([$this->message]);

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('exists')
            ->willReturn(true);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->setContainer($this->createMock(Container::class));

        $this->assertTrue($this->queue->pop('test') instanceof PubSubJob);
    }

    public function testPopWhenNoJobAvailablePullTimeout()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
                      ->setConstructorArgs([$this->client, 'default', ['pull_timeout' => 1]])
                      ->setMethods(['getTopic'])
                      ->getMock();

        $this->subscription->method('pull')
                           ->willReturn([]);

        $this->topic->method('subscription')
                    ->willReturn($this->subscription);

        $this->topic->method('exists')
                    ->willReturn(true);

        $queue->method('getTopic')
              ->willReturn($this->topic);

        $this->assertTrue(is_null($queue->pop('test')));
    }

    public function testGetSize()
    {
        $this->assertEquals(0, $this->queue->size());
    }

    public function testPopWithAcknowledgeDeadline()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
                      ->setConstructorArgs([$this->client, 'default', ['acknowledge_deadline' => 30]])
                      ->setMethods(['getTopic'])
                      ->getMock();

        $this->subscription->method('pull')
                           ->willReturn([]);

        $this->topic->method('subscription')
                    ->willReturn($this->subscription);

        $this->topic->method('exists')
                    ->willReturn(true);

        $queue->method('getTopic')
                    ->willReturn($this->topic);
        $this->assertEquals(30, $queue->getAcknowledgeDeadline());
        $this->assertTrue(is_null($queue->pop('test')));
    }

    public function testPopWithoutAcknowledgeDeadline()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
                      ->setConstructorArgs([$this->client, 'default'])
                      ->setMethods(['getTopic'])
                      ->getMock();

        $this->subscription->method('pull')
                           ->willReturn([]);

        $this->topic->method('subscription')
                    ->willReturn($this->subscription);

        $this->topic->method('exists')
                    ->willReturn(true);

        $queue->method('getTopic')
              ->willReturn($this->topic);
        $this->assertEquals(60, $queue->getAcknowledgeDeadline());
        $this->assertTrue(is_null($queue->pop('test')));
    }

    public function testPopWhenNoJobAvailable()
    {
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

    public function testPopWhenTopicDoesNotExist()
    {
        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->topic->method('exists')
            ->willReturn(false);

        $this->assertTrue(is_null($this->queue->pop('test')));
    }

    public function testBulk()
    {
        $jobs = ['test'];
        $data = ['foo' => 'bar'];

        $this->topic->expects($this->once())
            ->method('publishBatch')
            ->willReturn($this->result)
            ->with($this->callback(function ($payloads) use ($jobs, $data) {
                $decoded_payload = json_decode(base64_decode($payloads[0]['data']), true);

                return $decoded_payload['job'] === $jobs[0] && $decoded_payload['data'] === $data;
            }));

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $this->assertEquals($this->result, $this->queue->bulk($jobs, $data));
    }

    public function testAcknowledge()
    {
        $this->subscription->expects($this->once())
            ->method('acknowledge');

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->acknowledge($this->message);
    }

    public function testAcknowledgeAndPublish()
    {
        $options = ['foo' => 'bar'];
        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();

        $this->subscription->expects($this->once())
            ->method('acknowledge');

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->topic->expects($this->once())
            ->method('publish')
            ->willReturn($this->result)
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

        $this->queue->acknowledgeAndPublish($this->message, 'test', $options, $delay);
    }

    public function testAcknowledgeAndPublishOptionsOnlyAcceptString()
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
            ->willReturn($this->result);

        $options = [
            'integer' => 42,
            'array' => [
                'foo' => 'bar',
            ],
            1 => 'wrong key',
            'object' => new \StdClass,
        ];

        $this->queue->acknowledgeAndPublish($this->message, 'test', $options, $delay);
    }

    public function testGetTopic()
    {
        $this->topic->method('exists')
            ->willReturn(true);

        $this->client->method('topic')
            ->willReturn($this->topic);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->getTopic('test') instanceof Topic);
    }

    public function testCreateTopicAndReturnIt()
    {
        $this->topic->method('exists')
            ->willReturn(false);

        $this->topic->expects($this->once())
            ->method('create')
            ->willReturn(true);

        $this->client->method('topic')
            ->willReturn($this->topic);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->getTopic('test', true) instanceof Topic);
    }

    public function testSubscribtionIsCreated()
    {
        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->topic->method('subscribe')
            ->willReturn($this->subscription);

        $this->subscription->method('exists')
            ->willReturn(false);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->subscribeToTopic($this->topic) instanceof Subscription);
    }

    public function testSubscriptionIsRetrieved()
    {
        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->subscription->method('exists')
            ->willReturn(true);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default'])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->subscribeToTopic($this->topic) instanceof Subscription);
    }

    public function testGetSubscriberName()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', ['subscriber' => 'test-subscriber']])
            ->setMethods()
            ->getMock();

        $this->assertTrue(is_string($queue->getSubscriberName()));
        $this->assertEquals($queue->getSubscriberName(), 'test-subscriber');
    }

    public function testGetPubSub()
    {
        $this->assertTrue($this->queue->getPubSub() instanceof PubSubClient);
    }
}

<?php

namespace PubSub\PubSubQueue\Tests\Unit;

use Carbon\Carbon;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use PHPUnit\Framework\TestCase;
use PubSub\PubSubQueue\Jobs\PubSubJob;
use PubSub\PubSubQueue\PubSubQueue;
use ReflectionClass;

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
        $this->config = [
            'queue' => 'test',
            'project_id' => 'the-project-id',
            'retries' => 1,
            'request_timeout' => 60,
            'subscribers' => [
                'sub1' => 'topic1',
                'sub2' => 'topic2',
                'sub3' => 'topic1',
            ],
        ];
        $this->queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods([
                'pushRaw',
                'getTopic',
                'getTopicUsingSubscriber',
                'exists',
                'subscription',
                'availableAt',
                'subscribeToTopic',
                'getQueue',
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
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods(['getTopicUsingSubscriber', 'subscribeToTopic'])
            ->getMock();

        $this->topic->method('publish')
            ->willReturn($this->result);

        $queue->method('getTopicUsingSubscriber')
            ->willReturn($this->topic);

        $queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $payload = json_encode(['id' => $this->result]);

        $this->assertEquals($this->result, $queue->pushRaw($payload));
    }

    public function testLater()
    {
        $job = 'test';
        $delay = 60;
        $delay_timestamp = Carbon::now()->addSeconds($delay)->getTimestamp();
        $delay_timestamp_string = (string) $delay_timestamp;

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->queue->expects($this->once())
            ->method('pushRaw')
            ->willReturn($this->result)
            ->with(
                $this->isType('string'),
                $this->anything(),
                $this->callback(function ($options) use ($delay_timestamp_string) {
                    if (!isset($options['available_at']) || $options['available_at'] !== $delay_timestamp_string) {
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

    // public function testPopWhenTopicDoesNotExist()
    // {
    //     $this->queue->method('getTopicUsingSubscriber')
    //         ->willReturn($this->topic);

    //     $this->topic->method('exists')
    //         ->willReturn(false);

    //     $this->assertTrue(is_null($this->queue->pop('test')));
    // }

    public function testBulk()
    {
        $this->topic->expects($this->once())
            ->method('publishBatch')
            ->willReturn($this->result);

        $this->queue->method('getTopic')
            ->willReturn($this->topic);

        $this->queue->method('subscribeToTopic')
            ->willReturn($this->subscription);

        $this->assertEquals($this->result, $this->queue->bulk(['test'], ['foo' => 'bar']));
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
        $delay_timestamp_string = (string) $delay_timestamp;

        $this->subscription->expects($this->once())
            ->method('acknowledge');

        $this->topic->method('subscription')
            ->willReturn($this->subscription);

        $this->queue->method('getTopicUsingSubscriber')
            ->willReturn($this->topic);

        $this->queue->method('availableAt')
            ->willReturn($delay_timestamp);

        $this->topic->expects($this->once())
            ->method('publish')
            ->willReturn($this->result)
            ->with(
                $this->callback(function ($message) use ($options, $delay_timestamp_string) {
                    if (!isset($message['attributes'])) {
                        return false;
                    }

                    if (!isset($message['attributes']['available_at']) || $message['attributes']['available_at'] !== $delay_timestamp_string) {
                        return false;
                    }

                    if (!isset($message['attributes']['foo']) || $message['attributes']['foo'] != $options['foo']) {
                        return false;
                    }

                    return true;
                })
            );

        $this->queue->acknowledgeAndPublish($this->message, 'test', $options, $delay);
    }

    public function testGetTopic()
    {
        $this->topic->method('exists')
            ->willReturn(true);

        $this->client->method('topic')
            ->willReturn($this->topic);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->getTopic('test') instanceof Topic);
    }

    public function testGetTopicUsingSubscriber()
    {
        $this->client->method('topic')
            ->willReturn($this->topic);

        $this->queue->method('getQueue')
            ->willReturn($this->topic);

        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods()
            ->getMock();
        $this->assertTrue($queue->getTopicUsingSubscriber('sub1') instanceof Topic);

    }

    public function testGetQueue()
    {
        $queue = $this->getMockBuilder(PubSubQueue::class)
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->getQueue('sub1') === 'topic1');

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
            ->setConstructorArgs([$this->client, 'default', $this->config])
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
            ->setConstructorArgs([$this->client, 'default', $this->config])
            ->setMethods()
            ->getMock();

        $this->assertTrue($queue->subscribeToTopic($this->topic) instanceof Subscription);
    }

    public function testGetPubSub()
    {
        $this->assertTrue($this->queue->getPubSub() instanceof PubSubClient);
    }

}

<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit\PubSub\Topics;

use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Kainxspirits\PubSubQueue\PubSub\Topics\TopicProxy;
use PHPUnit\Framework\TestCase;

class TopicProxyTests extends TestCase
{
    protected $topic;

    public function setUp() : void
    {
        $this->topic = $this->createMock(Topic::class);
    }

    public function testItCanPublish()
    {
        $this->topic->expects($this->once())->method('publish')->willReturn('message-id');

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->publish(['id' => 'test']);
    }

    public function testItRetriesPublishIfNotFound()
    {
        $this->topic->expects($this->at(0))->method('publish')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->at(1))->method('publish')->willReturn('message-id');
        $this->topic->expects($this->once())->method('subscribe');
        $this->topic->expects($this->once())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->publish(['id' => 'test']);
    }

    public function testItDoesntAutocreateTopicOnPublish()
    {
        $this->topic->expects($this->any())->method('publish')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->never())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', false, true);

        $this->expectException(NotFoundException::class);

        $proxy->publish(['id' => 'test']);
    }

    public function testItDoesntAutosubscribeTopicOnPublish()
    {
        $this->topic->expects($this->any())->method('exists')->willreturn(true);
        $this->topic->expects($this->any())->method('publish')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->never())->method('subscribe');

        $proxy = new TopicProxy($this->topic, 'test', true, false);

        $this->expectException(NotFoundException::class);

        $proxy->publish(['id' => 'test']);
    }

    public function testItCanPublishBatch()
    {
        $this->topic->expects($this->once())->method('publishBatch')->willReturn(['message-1', 'message-2']);

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->publishBatch([['id' => 'test1'], ['id' => 'test2']]);
    }

    public function testItRetriesPublishBatchIfNotFound()
    {
        $this->topic->expects($this->at(0))->method('publishBatch')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->at(1))->method('publishBatch')->willReturn(['message-1', 'message-2']);
        $this->topic->expects($this->once())->method('subscribe');
        $this->topic->expects($this->once())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->publishBatch([['id' => 'test1'], ['id' => 'test2']]);
    }

    public function testItDoesntAutocreateTopicOnPublishBatch()
    {
        $this->topic->expects($this->any())->method('publishBatch')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->never())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', false, true);

        $this->expectException(NotFoundException::class);

        $proxy->publishBatch([['id' => 'test1'], ['id' => 'test2']]);
    }

    public function testItDoesntAutosubscribeTopicOnPublishBatch()
    {
        $this->topic->expects($this->any())->method('exists')->willreturn(true);
        $this->topic->expects($this->any())->method('publishBatch')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->never())->method('subscribe');

        $proxy = new TopicProxy($this->topic, 'test', true, false);

        $this->expectException(NotFoundException::class);

        $proxy->publishBatch([['id' => 'test1'], ['id' => 'test2']]);
    }

    public function testItCanSubscribe()
    {
        $this->topic->expects($this->once())->method('subscribe')->willReturn($this->createMock(Subscription::class));

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->subscribe('test');
    }

    public function testItRetriesSubscribeIfNotFound()
    {
        $this->topic->expects($this->at(0))->method('subscribe')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->at(1))->method('subscribe')->willReturn($this->createMock(Subscription::class));
        $this->topic->expects($this->once())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', true, true);

        $proxy->subscribe('test');
    }

    public function testItDoesntAutocreateTopicOnSubscribe()
    {
        $this->topic->expects($this->any())->method('subscribe')->willThrowException(new NotFoundException('test'));
        $this->topic->expects($this->never())->method('create');

        $proxy = new TopicProxy($this->topic, 'test', false, true);

        $this->expectException(NotFoundException::class);

        $proxy->subscribe('test');
    }

    public function testItCanGetTopic()
    {
        $proxy = new TopicProxy($this->topic, 'test', false, true);

        $this->assertEquals($this->topic, $proxy->getTopic());
    }
}

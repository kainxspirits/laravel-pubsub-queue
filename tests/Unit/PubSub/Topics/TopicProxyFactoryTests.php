<?php

namespace Kainxspirits\PubSubQueue\Tests\Unit\PubSub\Topics;

use Google\Cloud\PubSub\Topic;
use Kainxspirits\PubSubQueue\PubSub\Topics\TopicProxy;
use Kainxspirits\PubSubQueue\PubSub\Topics\TopicProxyFactory;
use PHPUnit\Framework\TestCase;

class TopicProxyFactoryTests extends TestCase
{
    public function testMakeReturnsTestProxy()
    {
        $topic = $this->createMock(Topic::class);

        $factory = new TopicProxyFactory;
        $proxy = $factory->make($topic, 'test');

        $this->assertTrue($proxy instanceof TopicProxy);
    }
}

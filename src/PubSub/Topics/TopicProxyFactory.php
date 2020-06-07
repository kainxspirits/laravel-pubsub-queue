<?php

namespace Kainxspirits\PubSubQueue\PubSub\Topics;

use Google\Cloud\PubSub\Topic;

class TopicProxyFactory
{
    /**
     * @var bool
     */
    protected $createsTopics;

    /**
     * @var bool
     */
    protected $createsSubscriptions;

    /**
     * @param bool $createsTopics
     * @param bool $createsSubscriptions
     */
    public function __construct(bool $createsTopics = true, bool $createsSubscriptions = true)
    {
        $this->createsTopics = $createsTopics;
        $this->createsSubscriptions = $createsSubscriptions;
    }

    /**
     *  @param \Google\Cloud\PubSub\Topic $topic
     *  @param string $subscriptionName
     *
     * @return TopicProxy
     */
    public function make(Topic $topic, string $subscriptionName): TopicProxy
    {
        return new TopicProxy(
            $topic,
            $subscriptionName,
            $this->createsTopics,
            $this->createsSubscriptions
        );
    }
}

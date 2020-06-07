<?php

namespace Kainxspirits\PubSubQueue\PubSub\Topics;

use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\PubSub\Topic;

class TopicProxy
{
    /**
     * @var \Google\Cloud\PubSub\Topic
     */
    protected $topic;

    /**
     * @var string
     */
    protected $subscriptionName;

    /**
     * @var bool
     */
    protected $createsTopics;

    /**
     * @var bool
     */
    protected $createsSubscriptions;

    /**
     * @param \Google\Cloud\PubSub\Topic $topic
     * @param string $subscriptionName
     * @param bool $createsTopics
     * @param bool $createsSubscriptions
     */
    public function __construct(Topic $topic, string $subscriptionName, bool $createsTopics = true, bool $createsSubscriptions = true)
    {
        $this->topic = $topic;
        $this->subscriptionName = $subscriptionName;
        $this->createsTopics = $createsTopics;
        $this->createsSubscriptions = $createsSubscriptions;
    }

    /**
     * @see \Google\Cloud\PubSub\Topic::publish
     *
     * @param  \Google\Cloud\PubSub\Message|array $message
     * @param  array  $options
     *
     * @return array
     */
    public function publish($message, array $options = [])
    {
        try {
            return $this->topic->publish($message, $options);
        } catch (NotFoundException $exception) {
            return $this->handleNotFoundException($this->topic)->publish($message, $options);
        }
    }

    /**
     * @see \Google\Cloud\PubSub\Topic::publishBatch
     *
     * @param  \Google\Cloud\PubSub\Message[]|array[] $messages
     * @param  array  $options
     *
     * @return array
     */
    public function publishBatch(array $messages, array $options = [])
    {
        try {
            return $this->topic->publishBatch($messages, $options);
        } catch (NotFoundException $exception) {
            return $this->handleNotFoundException($this->topic)->publishBatch($messages, $options);
        }
    }

    /**
     * @see \Google\Cloud\PubSub\Topic::subscribe
     *
     * @param  string $name
     * @param  array  $options
     *
     * @return Subscription
     */
    public function subscribe($name, array $options = [])
    {
        $subscriptionCreationConfiguration = $this->createsSubscriptions;
        $this->createsSubscriptions = false;

        try {
            return $this->topic->subscribe($name, $options);
        } catch (NotFoundException $exception) {
            return $this->handleNotFoundException($this->topic)->subscribe($name, $options);
        } finally {
            $this->createsSubscriptions = $subscriptionCreationConfiguration;
        }
    }

    /**
     * @return \Google\Cloud\PubSub\Topic
     */
    public function getTopic() : Topic
    {
        return $this->topic;
    }

    /**
     * @param  \Google\Cloud\PubSub\Topic $topic
     *
     * @return \Google\Cloud\PubSub\Topic
     */
    protected function handleNotFoundException(Topic $topic) : Topic
    {
        $topicExists = $topic->exists();

        if ($this->shouldRecreateTopics() && ! $topicExists) {
            $topic->create();
            $this->topic = $topic;
        }

        $subscription = $this->topic->subscription($this->subscriptionName);

        if (! $topicExists || ($this->shouldRecreateSubscriptions() && ! $subscription->exists())) {
            $this->topic->subscribe($this->subscriptionName);
        }

        return $this->topic;
    }

    /**
     * @return bool
     */
    protected function shouldRecreateTopics() : bool
    {
        return $this->createsTopics;
    }

    /**
     * @return bool
     */
    protected function shouldRecreateSubscriptions() : bool
    {
        return $this->createsSubscriptions;
    }
}

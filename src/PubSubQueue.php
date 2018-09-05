<?php

namespace Kainxspirits\PubSubQueue;

use Illuminate\Queue\Queue;
use Google\Cloud\PubSub\Topic;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Kainxspirits\PubSubQueue\Jobs\PubSubJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class PubSubQueue extends Queue implements QueueContract
{
    /**
     * The PubSubClient instance.
     *
     * @var \Google\Cloud\PubSub\PubSubClient
     */
    protected $pubsub;

    /**
     * Default queue name.
     *
     * @var string
     */
    protected $default;

    /**
     * Create a new GCP PubSub instance.
     *
     * @param \Google\Cloud\PubSub\PubSubClient $pubsub
     * @param string $default
     */
    public function __construct(PubSubClient $pubsub, $default)
    {
        $this->pubsub = $pubsub;
        $this->default = $default;
    }

    /**
     * Get the size of the queue.
     * PubSubClient have no method to retrieve the size of the queue.
     * To be updated if the API allow to get that data.
     *
     * @param  string  $queue
     *
     * @return int
     */
    public function size($queue = null)
    {
        return 0;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string|object  $job
     * @param  mixed   $data
     * @param  string  $queue
     *
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string  $queue
     * @param  array   $options
     *
     * @return array
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $topic = $this->getTopic($queue, true);

        $this->subscribeToTopic($topic);

        return $topic->publish([
            'data' => $payload,
            'attributes' => $options,
        ]);
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  string|object  $job
     * @param  mixed   $data
     * @param  string  $queue
     *
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->pushRaw(
            $this->createPayload($job, $data),
            $queue,
            ['available_at' => $this->availableAt($delay)]
        );
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $topic = $this->getTopic($queue);

        if (! $topic->exists()) {
            return;
        }

        $subscription = $topic->subscription($this->getSubscriberName());
        $messages = $subscription->pull([
            'returnImmediately' => true,
            'maxMessages' => 1,
        ]);

        if (! empty($messages) && count($messages) > 0) {
            return new PubSubJob(
                $this->container,
                $this,
                $messages[0],
                $this->connectionName,
                $queue
            );
        } else {
            $subscription->delete();
        }
    }

    /**
     * Push an array of jobs onto the queue.
     *
     * @param  array   $jobs
     * @param  mixed   $data
     * @param  string  $queue
     *
     * @return mixed
     */
    public function bulk($jobs, $data = '', $queue = null)
    {
        $payloads = [];

        foreach ((array) $jobs as $job) {
            $payloads[] = ['data' => $this->createPayload($job, $data)];
        }

        $topic = $this->getTopic($queue, true);

        $this->subscribeToTopic($topic);

        return $topic->publishBatch($payloads);
    }

    /**
     * Acknowledge a message.
     *
     * @param  \Google\Cloud\PubSub\Message $message
     * @param  string $queue
     */
    public function acknowledge(Message $message, $queue = null)
    {
        $subscription = $this->getTopic($queue)->subscription($this->getSubscriberName());
        $subscription->acknowledge($message);
    }

    /**
     * Acknowledge a message and republish it onto the queue.
     *
     * @param  \Google\Cloud\PubSub\Message $message
     * @param  string $queue
     *
     * @return mixed
     */
    public function acknowledgeAndPublish(Message $message, $queue = null, $options = [], $delay = 0)
    {
        $topic = $this->getTopic($queue);
        $subscription = $topic->subscription($this->getSubscriberName());

        $subscription->acknowledge($message);

        $options = array_merge([
            'available_at' => $this->availableAt($delay),
        ], $options);

        return $topic->publish([
            'data' => $message->data(),
            'attributes' => $options,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    protected function createPayload($job, $data = '')
    {
        $payload = parent::createPayload($job, $data);

        return base64_encode($payload);
    }

    /**
     * Get the current topic.
     *
     * @param  string $queue
     * @param  string $create
     *
     * @return \Google\Cloud\PubSub\Topic
     */
    public function getTopic($queue, $create = false)
    {
        $queue = $queue ?: $this->default;
        $topic = $this->pubsub->topic($queue);

        if (! $topic->exists() && $create) {
            $topic->create();
        }

        return $topic;
    }

    /**
     * Create a new subscription to a topic.
     *
     * @param  \Google\Cloud\PubSub\Topic  $topic
     *
     * @return \Google\Cloud\PubSub\Subscription
     */
    public function subscribeToTopic(Topic $topic)
    {
        $subscription = $topic->subscription($this->getSubscriberName());

        if (! $subscription->exists()) {
            $subscription = $topic->subscribe($this->getSubscriberName());
        }

        return $subscription;
    }

    /**
     * Get subscriber name.
     *
     * @param  \Google\Cloud\PubSub\Topic  $topic
     *
     * @return string
     */
    public function getSubscriberName()
    {
        return 'subscriber';
    }

    /**
     * Get the PubSub instance.
     *
     * @return \Google\Cloud\PubSub\PubSubClient
     */
    public function getPubSub()
    {
        return $this->pubsub;
    }
}

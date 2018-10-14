<?php

namespace Kainxspirits\PubSubQueue;

use Illuminate\Queue\Queue;
use Illuminate\Support\Str;
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
        return $this->pushRaw($this->createPayload($job, $this->getQueue($queue), $data), $queue);
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

        $publish = ['data' => $payload];

        if (! empty($options)) {
            $publish['attributes'] = $options;
        }

        $topic->publish($publish);

        $decoded_payload = json_decode(base64_decode($payload), true);

        return $decoded_payload['id'];
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
            $this->createPayload($job, $this->getQueue($queue), $data),
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
        $topic = $this->getTopic($this->getQueue($queue));

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
                $this->getQueue($queue)
            );
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
            $payloads[] = ['data' => $this->createPayload($job, $this->getQueue($queue), $data)];
        }

        $topic = $this->getTopic($this->getQueue($queue), true);

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
        $subscription = $this->getTopic($this->getQueue($queue))->subscription($this->getSubscriberName());
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
        $topic = $this->getTopic($this->getQueue($queue));
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
     * Create a payload string from the given job and data.
     *
     * @param  string  $job
     * @param  string  $queue
     * @param  mixed   $data
     * @return string
     *
     * @throws \Illuminate\Queue\InvalidPayloadException
     */
    protected function createPayload($job, $queue, $data = '')
    {
        $payload = parent::createPayload($job, $this->getQueue($queue), $data);

        return base64_encode($payload);
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  mixed  $job
     * @param  string  $queue
     * @param  mixed  $data
     * @return array
     */
    protected function createPayloadArray($job, $queue, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $this->getQueue($queue), $data), [
            'id' => $this->getRandomId(),
        ]);
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
        $queue = $this->getQueue($queue);
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

    /**
     * Get the queue or return the default.
     *
     * @param  string|null  $queue
     * @return string
     */
    public function getQueue($queue)
    {
        return $queue ?: $this->default;
    }

    /**
     * Get a random ID string.
     *
     * @return string
     */
    protected function getRandomId()
    {
        return Str::random(32);
    }
}

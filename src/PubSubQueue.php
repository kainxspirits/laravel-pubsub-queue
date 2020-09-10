<?php

namespace Kainxspirits\PubSubQueue;

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Str;
use Kainxspirits\PubSubQueue\Jobs\PubSubJob;

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
     * Default subscriber.
     *
     * @var string
     */
    protected $subscriber;

    /**
     * Create a new GCP PubSub instance.
     *
     * @param \Google\Cloud\PubSub\PubSubClient $pubsub
     * @param string $default
     */
    public function __construct(PubSubClient $pubsub, $default, $subscriber = 'subscriber')
    {
        $this->pubsub = $pubsub;
        $this->default = $default;
        $this->subscriber = $subscriber;
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

        $publish = ['data' => base64_encode($payload)];

        if (! empty($options)) {
            $publish['attributes'] = $this->validateMessageAttributes($options);
        }

        $topic->publish($publish);

        $decoded_payload = json_decode($payload, true);

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
            ['available_at' => (string) $this->availableAt($delay)]
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

        if (empty($messages) || count($messages) < 1) {
            return;
        }

        $available_at = intval($messages[0]->attribute('available_at'));
        $current_time = time();
        if ($available_at && $available_at > $current_time) {
            $this->modifyAckDeadline($messages[0], $available_at - $current_time, $queue);

            return;
        }

        $this->acknowledge($messages[0], $queue);

        return new PubSubJob(
            $this->container,
            $this,
            $messages[0],
            $this->connectionName,
            $this->getQueue($queue)
        );
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
            $payload = $this->createPayload($job, $this->getQueue($queue), $data);
            $payloads[] = ['data' => base64_encode($payload)];
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
     * Modify acknowledge deadline of a message.
     *
     * @param  \Google\Cloud\PubSub\Message $message
     * @param  int $seconds
     * @param  string $queue
     */
    public function modifyAckDeadline(Message $message, int $seconds, $queue = null)
    {
        $subscription = $this->getTopic($this->getQueue($queue))->subscription($this->getSubscriberName());
        $subscription->modifyAckDeadline($message, $seconds);
    }

    /**
     * Republish a message onto the queue.
     *
     * @param  \Google\Cloud\PubSub\Message $message
     * @param  string $queue
     *
     * @return mixed
     */
    public function republish(Message $message, $queue = null, $options = [], $delay = 0)
    {
        $topic = $this->getTopic($this->getQueue($queue));

        $options = array_merge([
            'available_at' => (string) $this->availableAt($delay),
        ], $this->validateMessageAttributes($options));

        return $topic->publish([
            'data' => $message->data(),
            'attributes' => $options,
        ]);
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
     * Check if the attributes array only contains key-values
     * pairs made of strings.
     *
     * @param  array $attributes
     *
     * @throws \UnexpectedValueException
     * @return array
     */
    private function validateMessageAttributes($attributes): array
    {
        $attributes_values = array_filter($attributes, 'is_string');

        if (count($attributes_values) !== count($attributes)) {
            throw new \UnexpectedValueException('PubSubMessage attributes only accept key-value pairs and all values must be string.');
        }

        $attributes_keys = array_filter(array_keys($attributes), 'is_string');

        if (count($attributes_keys) !== count(array_keys($attributes))) {
            throw new \UnexpectedValueException('PubSubMessage attributes only accept key-value pairs and all keys must be string.');
        }

        return $attributes;
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
        return $this->subscriber;
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

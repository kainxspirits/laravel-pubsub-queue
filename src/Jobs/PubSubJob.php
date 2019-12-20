<?php

namespace PubSub\PubSubQueue\Jobs;

use Google\Cloud\PubSub\Message;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use PubSub\PubSubQueue\PubSubQueue;

class PubSubJob extends Job implements JobContract
{
    /**
     * The PubSub queue.
     *
     * @var \PubSub\PubSubQueue\PubSubQueue
     */
    protected $pubsub;

    /**
     * The job instance.
     *
     * @var array
     */
    protected $job;

    /**
     * subscriber name
     *
     * @var string
     */
    protected $subscriber;

    /**
     * Create a new job instance.
     *
     * @param \Illuminate\Container\Container $container
     * @param \PubSub\PubSubQueue\PubSubQueue $sqs
     * @param \Google\Cloud\PubSub\Message $job
     * @param string       $connectionName
     * @param string       $queue
     */
    public function __construct(Container $container, PubSubQueue $pubsub, Message $job, $connectionName, $subscriberName, $subscriber = null)
    {
        $this->pubsub = $pubsub;
        $this->job = $job;
        $this->queue = $subscriberName;
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->subscriber = $subscriber;
        $this->decoded = $this->payload();
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

    /**
     * Get the raw body of the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        if ($this->pubsub->checkHandler($this->subscriber)) {
            return $this->modifyPayload(
                $this->job->data(),
                $this->pubsub->getHandler($this->subscriber)
            );
        }
        return base64_decode($this->job->data());
    }

    /**
     * @param string|array $payload
     * @param string $class
     * @return array
     */
    private function modifyPayload($payload, $class)
    {
        if (!is_array($payload)) {
            $payload = json_decode($payload, true);
        }

        $body = [
            'job' => $class . '@handle',
            'data' => $payload,
        ];

        return json_encode($body);
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->pubsub->acknowledge($this->job, $this->queue);
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return ((int) $this->job->attribute('attempts') ?? 0) + 1;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int   $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $attempts = $this->attempts();

        $this->pubsub->acknowledgeAndPublish(
            $this->job,
            $this->queue,
            ['attempts' => $attempts],
            $delay
        );
    }
}

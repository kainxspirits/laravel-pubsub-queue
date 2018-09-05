<?php

namespace Kainxspirits\PubSubQueue\Jobs;

use Illuminate\Queue\Jobs\Job;
use Google\Cloud\PubSub\Message;
use Illuminate\Container\Container;
use Kainxspirits\PubSubQueue\PubSubQueue;
use Illuminate\Contracts\Queue\Job as JobContract;

class PubSubJob extends Job implements JobContract
{
    /**
     * The PubSub queue.
     *
     * @var \Kainxspirits\PubSubQueue\PubSubQueue
     */
    protected $pubsub;

    /**
     * The job instance.
     *
     * @var array
     */
    protected $job;

    /**
     * Create a new job instance.
     *
     * @param \Illuminate\Container\Container $container
     * @param \Kainxspirits\PubSubQueue\PubSubQueue $sqs
     * @param \Google\Cloud\PubSub\Message $job
     * @param string       $connectionName
     * @param string       $queue
     */
    public function __construct(Container $container, PubSubQueue $pubsub, Message $job, $connectionName, $queue)
    {
        $this->pubsub = $pubsub;
        $this->job = $job;
        $this->queue = $queue;
        $this->container = $container;
        $this->connectionName = $connectionName;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->job->id();
    }

    /**
     * Get the raw body of the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return base64_decode($this->job->data());
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
        return (int) $this->job->attributes('attempts') ?? 0;
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

        $attempts = $this->attempts() + 1;
        $this->pubsub->acknowledgeAndPublish(
            $this->job,
            $this->queue,
            ['attempts' => $attempts],
            $delay
        );
    }
}

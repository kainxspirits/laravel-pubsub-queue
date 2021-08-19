<?php

namespace Kainxspirits\PubSubQueue\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;

class PubSubConsume extends Command
{
    /**
     * @var string
     */
    protected $signature = 'pubsub:consume
                            {sub-name : The name of the sub to consume}
                            {--sleep=3 : Number of seconds to sleep when no job is available}';

    /**
     * @var string
     */
    protected $description = 'Start processing messages on the specified subscription';

    /**
     * @return void
     */
    public function handle(): void
    {
        $this->setSubscriptionToConsume($this->getSubscriptionName());

        Artisan::call('queue:work',
            ['connection' => 'pubsub', '--sleep' => $this->getSleepOption()],
            $this->output
        );
    }

    /**
     * @param string $subscriptionName
     *
     * @return void
     */
    private function setSubscriptionToConsume(string $subscriptionName): void
    {
        config(['queue.connections.pubsub.subscriber' => $subscriptionName]);
    }

    /**
     * @return string
     */
    private function getSubscriptionName(): string
    {
        return $this->argument('sub-name');
    }

    /**
     * @return string
     */
    private function getSleepOption(): string
    {
        return $this->option('sleep');
    }
}

<?php

namespace Kainxspirits\PubSubQueue;

use Illuminate\Support\ServiceProvider;
use Kainxspirits\PubSubQueue\Commands\PubSubConsume;
use Kainxspirits\PubSubQueue\Connectors\PubSubConnector;

class PubSubQueueServiceProvider extends ServiceProvider
{
    /**
     * @return void
     */
    public function boot(): void
    {
        $this->registerPubSubCommands();
        $this->addPubSubConnector();
    }

    /**
     * @return void
     */
    private function addPubSubConnector(): void
    {
        $this->app['queue']->addConnector('pubsub', static function () {
            return new PubSubConnector;
        });
    }

    /**
     * @return void
     */
    private function registerPubSubCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                PubSubConsume::class,
            ]);
        }
    }
}

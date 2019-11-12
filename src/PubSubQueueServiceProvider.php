<?php

namespace Kainxspirits\PubSubQueue;

use Illuminate\Support\ServiceProvider;
use Kainxspirits\PubSubQueue\Connectors\PubSubConnector;

class PubSubQueueServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot()
    {
        $cache = $this->app['cache'] ?? null;
        $this->app['queue']->addConnector('pubsub', function () use ($cache) {
            return new PubSubConnector($cache);
        });
    }
}

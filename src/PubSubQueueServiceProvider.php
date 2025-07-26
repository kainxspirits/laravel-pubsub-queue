<?php

namespace Kainxspirits\PubSubQueue;

use Illuminate\Support\Facades\Queue;
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
        Queue::addConnector('pubsub', static fn () => new PubSubConnector);
    }
}

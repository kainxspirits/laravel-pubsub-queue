<?php

namespace Kainxspirits\PubSubQueue\Connectors;

use Google\Cloud\PubSub\PubSubClient;
use Kainxspirits\PubSubQueue\PubSubQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

class PubSubConnector implements ConnectorInterface
{
    /**
     * Default queue name.
     *
     * @var string
     */
    protected $default_queue = 'default';

    /**
     * Establish a queue connection.
     *
     * @param  array  $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $gcp_config = $this->transformConfig($config);

        return new PubSubQueue(
            new PubSubClient($gcp_config),
            $config['queue'] ?? $this->default_queue,
            $config['subscriber'] ?? 'subscriber'
        );
    }

    /**
     * Transform the config to key => value array.
     *
     * @param  array $config
     *
     * @return array
     */
    protected function transformConfig($config)
    {
        return array_reduce(array_map([$this, 'transformConfigKeys'], $config, array_keys($config)), function ($carry, $item) {
            $carry[$item[0]] = $item[1];

            return $carry;
        }, []);
    }

    /**
     * Transform the keys of config to camelCase.
     *
     * @param  string $item
     * @param  string $key
     *
     * @return array
     */
    protected function transformConfigKeys($item, $key)
    {
        return [camel_case($key), $item];
    }
}

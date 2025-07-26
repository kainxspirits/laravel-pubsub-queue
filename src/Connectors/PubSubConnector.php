<?php

namespace Kainxspirits\PubSubQueue\Connectors;

use Google\Cloud\PubSub\PubSubClient;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Kainxspirits\PubSubQueue\PubSubQueue;

class PubSubConnector implements ConnectorInterface
{
    protected static $exceptKeys = [
        'subscriber',
        'requestTimeout',
        'createTopics',
        'createSubscriptions',
        'useQueueAsSubscriber',
        'queuePrefix',
        'queue',
        'accessToken',
        'shouldSignRequest',
        'preferNumericProjectId',
        'asyncHttpHandler',
        'delayFunc',
        'calcDelayFunction',
    ];

    protected static $replacedKeys = [
        'restOptions' => 'transportConfig.rest',
        'grpcOptions' => 'transportConfig.grpc',
        'httpHandler' => 'transportConfig.rest.httpHandler',
        'authHttpHandler' => 'credentialsConfig.authHttpHandler',
        'quotaProject' => 'credentialsConfig.quotaProject',
        'defaultScopes' => 'credentialsConfig.defaultScopes',
        'scopes' => 'credentialsConfig.scopes',
        'keyFile' => 'credentials',
        'keyFilePath' => 'credentials',
        'credentialsFetcher' => 'credentials',
        'authCacheOptions' => 'credentialsConfig.authCacheOptions',
        'authCache' => 'credentialsConfig.authCache',
        'retries' => 'retrySettings.maxRetries',
        'restRetryFunction' => 'retrySettings.retryFunction',
        'grpcRetryFunction' => 'retrySettings.retryFunction',
    ];

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
            $config
        );
    }

    /**
     * Transform the config to key => value array.
     *
     * @param  array  $config
     * @return array
     */
    protected function transformConfig($config)
    {
        $gcpConfig = array_reduce(array_map([$this, 'transformConfigKeys'], $config, array_keys($config)), function ($carry, $item) {
            $carry[$item[0]] = $item[1];

            return $carry;
        }, []);

        foreach ($gcpConfig as $key => $value) {
            if (in_array($key, static::$exceptKeys, true)) {
                unset($gcpConfig[$key]);
                continue;
            }

            if (array_key_exists($key, static::$replacedKeys)) {
                Arr::set($gcpConfig, static::$replacedKeys[$key], $value);
                unset($gcpConfig[$key]);
            }
        }

        return $gcpConfig;
    }

    /**
     * Transform the keys of config to camelCase.
     *
     * @param  string  $item
     * @param  string  $key
     * @return array
     */
    protected function transformConfigKeys($item, $key)
    {
        return [Str::camel($key), $item];
    }
}

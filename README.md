# Laravel PubSub Queue

[![Travis](https://img.shields.io/travis/munir131/laravel-pubsub-queue/5.8?style=for-the-badge)](https://github.com/munir131/laravel-pubsub-queue)
[![StyleCI](https://styleci.io/repos/131718560/shield)](https://styleci.io/repos/131718560)

This package is a Laravel 5.8 queue driver that use the [Google PubSub](https://github.com/GoogleCloudPlatform/google-cloud-php-pubsub) service.

## Installation

You can easily install this package with [Composer](https://getcomposer.org) by running this command :

```bash
composer require munir131/laravel-pubsub-queue
```

If you disabled package discovery, you can still manually register this package by adding the following line to the providers of your `config/app.php` file :

```php
PubSub\PubSubQueue\PubSubQueueServiceProvider::class,
```

## Configuration

Add a `pubsub` connection to your `config/queue.php` file. From there, you can use any configuration values from the original pubsub client. Just make sure to use snake_case for the keys name.

You can check [Google Cloud PubSub client](http://googlecloudplatform.github.io/google-cloud-php/#/docs/google-cloud/v0.62.0/pubsub/pubsubclient?method=__construct) for more details about the different options.

```php
'pubsub' => [
            'driver' => 'pubsub',
            'queue' => env('JOB_PUB'),
            'project_id' => env('PUBSUB_PROJECT_ID', 'your-project-id'),
            'retries' => 5,
            'request_timeout' => 60,
            'keyFilePath' => env('PUBSUB_KEY_FILE', storage_path('Asyncro-978055.json')),
            // Subscriber will be key and publisher will be value here
            'subscribers' => [
                'sub1' => 'topic1',
                'sub2' => 'topic2',
                'sub3' => 'topic2' // Multiple subscriber to single topic
            ],
            'plain_handlers' => [
                'plain_sub' => App\Jobs\PlainClass::class // This one for non laravel format messages.
            ],
        ],
```

## Testing

You can run the tests with :

```bash
vendor/bin/phpunit
```

## License

This project is licensed under the terms of the MIT license. See [License File](LICENSE) for more information.

# Laravel PubSub Queue

[![Travis](https://img.shields.io/travis/kainxspirits/laravel-pubsub-queue.svg)](https://github.com/kainxspirits/laravel-pubsub-queue)
[![StyleCI](https://styleci.io/repos/131718560/shield)](https://styleci.io/repos/131718560)

This package is a Laravel 5.7 queue driver that use the [Google PubSub](https://github.com/GoogleCloudPlatform/google-cloud-php-pubsub) service.

## Installation

You can easily install this package with [Composer](https://getcomposer.org) by running this command :

```bash
composer require kainxspirits/laravel-pubsub-queue
```

If you disabled package discovery, you can still manually register this package by adding the following line to the providers of your `config/app.php` file :

```php
Kainxspirits\PubSubQueue\PubSubQueueServiceProvider::class,
```

## Configuration

Add a `pubsub` connection to your `config/queue.php` file. From there, you can use any configuration values from the original pubsub client. Just make sure to use snake_case for the keys name.

You can check [Google Cloud PubSub client](http://googleapis.github.io/google-cloud-php/#/docs/cloud-pubsub/master/pubsub/pubsubclient?method=__construct) for more details about the different options.

```php
'pubsub' => [
    'driver' => 'pubsub',
    'queue' => env('PUBSUB_QUEUE', 'default'),
    'project_id' => env('PUBSUB_PROJECT_ID', 'your-project-id'),
    'retries' => 3,
    'request_timeout' => 60,
    'subscriber' => 'subscriber-name',
],
```

#### Additional configuration settings: 

`acknowledge_deadline` The number of seconds, you want to set the acknowledge deadline to. If you run more then one worker for a configured queue. This *must* match or exceed the highest value of `--time-out` flag on all the workers for the configured queue.

`pull_timeout` Because PubSub can return 0 messages, even when there messages in the queue. You can set a timeout in seconds. This keeps the driver observing the PubSub subscription for the given time. You should only need this if you run a worker with the `--exit-on-empty` flag.
## Testing

You can run the tests with :

```bash
vendor/bin/phpunit
```

## License

This project is licensed under the terms of the MIT license. See [License File](LICENSE) for more information.

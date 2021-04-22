# Example KafkaJS Typescript App
This is a somewhat contrived example of using [KafkaJS](https://kafka.js.org/) to consume and produce events.
In particular this highlights the use case of unpacking a Kafka message, casting it to a domain model, then producing
it to some other topic.

* The `Configuration` class is a pattern my team and I are in the habit of using, and is not germane to KafkaJS usage
* The `Consumer` demonstrates batch and individual message consumption options

## Contributing
This example has been submitted to KafkaJS for use in their docs, but it is by no means perfect. If you have feedback,
please feel free to open an issue or pull request against this repo.

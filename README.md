# besluit-publicatie-melding-service
This service listens for resources of type [signing:PublishedResource][publishedResource], with status `<http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success>`
The service then notifies an endpoint, compliant with the [submission-api][submission-api]
In practice this will usually be [automatic-submission-service][automatic-submission-service].

## Rest API

- `POST /submit-publication`: endpoint for [mu-delta][mu-delta] to trigger. 
Can also be used to manually trigger the service for debugging purposes.


## Params
```
besluit-publicatie:
    image: lblod/besluit-publicatie-melding-service:z.y.x
    environment: # defaults params are shown here
      KEY: 'api-key-as-provided' # (required)
      SUBMISSION_ENDPOINT: 'http://automatic-submission-service/melding' # The target endpoint to submit to (required)
      SOURCE_HOST: 'http://the.public.host.where.publication.is.available' # (required)

      PUBLISHER_URI: 'http://data.lblod.info/vendors/gelinkt-notuleren'
      DEFAULT_GRAPH: 'http://mu.semte.ch/graphs/public'
      CACHING_CRON_PATTERN: '0 */5 * * * *'
      PENDING_TIMEOUT_HOURS: '3'
      MAX_ATTEMPTS: '10' 
      PING_DB_INTERVAL: '2'
    links:
      - database:database
```

#### KEY (required)

Provided api key for submitting to the [`SUBMISSION_ENDPOINT`][header-subend].

#### SUBMISSION_ENDPOINT (required)

URI for the endpoint where the notification (aka "melding") will be submitted to.
This endpoint must comply with the [submission-api][submission-api] specification.

#### SOURCE_HOST (required)

Base url where the published resource can be publicly viewed. Used for generating
urls which will be submitted to the [`SUBMISSION_ENDPOINT`][header-subend]. This should point to an instance of
[the publication stack][publication]

#### PUBLISHER_URI (default = `'http://data.lblod.info/vendors/gelinkt-notuleren'`)

Unique identifier of the publisher (aka creator) of the published resource. 
Together with the api key, this forms the authentication credentials for the notification stack.

#### DEFAULT_GRAPH (default = `'http://mu.semte.ch/graphs/public'`)

Default graph of the stack this service is a part of. Used to manage
running [task:Task][task] instances.

#### CACHING_CRON_PATTERN (default = `'0 */5 * * * *'`)

Currently not used.

#### PENDING_TIMEOUT_HOURS (default = `'3'`)

Currently not used.

#### MAX_ATTEMPTS (default = `'10'`)

Amount of times this service will retry to submit a notification to [`SUBMISSION_ENDPOINT`][header-subend] upon failure.

#### PING_DB_INTERVAL (default = `'2'`)

Time in seconds between pings to the database service while waiting for it
to start. Only relevant for startup, no effect once the stack is running.

## Example delta notifier config
```
export default [
  {
    match: {
      predicate: { type: "uri", value: "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status" },
      object: { type: "uri", value: "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success" }
    },
    callback: {
      url: "http://publicatie-melding/submit-publication", method: "POST"
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
]
```
## Inner workings
Once a valid resource is provided, a [task:Task][task] is created and processed. It retries in case of failure.
There is also a fallback, in case of issues with notifications

[header-subend]: #submission_endpoint-required
[submission-api]: https://lblod.github.io/pages-vendors/#/docs/submission-api
[publishedResource]: http://mu.semte.ch/vocabularies/ext/signing/PublishedResource
[automatic-submission-service]: https://github.com/lblod/automatic-submission-service
[mu-delta]: https://github.com/mu-semtech/delta-notifier
[task]: http://redpencil.data.gift/vocabularies/tasks/
[publication]: https://github.com/lblod/app-gn-publicatie

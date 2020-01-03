# besluit-publicatie-melding-service
This service listens for new publised
This service listens for resource [signing:PublishedResource](http://mu.semte.ch/vocabularies/ext/signing/PublishedResource), with status `<http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success>`
The service then notifies an endpoint, compliant with the [submission-api](https://lblod.github.io/pages-vendors/#/docs/submission-api).
In practice this should be [automatic-submission-service](https://github.com/lblod/automatic-submission-service).

## Rest API
For debugging:
- `POST /submit-publication`: endpoint for [mu-delta](https://github.com/mu-semtech/delta-notifier) to trigger.

## Params
```
besluit-publicatie:
    image: lblod/besluit-publicatie-melding-service:z.y.x
    environment: # defaults params are shown here
      CRON_FREQUENCY: '0 */5 * * * *' # As fallback in case issues with notifications
      KEY: 'api-key-as-provided' # (required)
      MAX_ATTEMPTS: '10'
      PUBLISHER_URI: '"http://data.lblod.info/vendors/gelinkt-notuleren'
      SUBMISSION_ENDPOINT: 'http://automatic-submission-service/melding' # The target endpoint to submit to (required)
      SOURCE_HOST: 'http://localhost:4202'
      DEFAULT_GRAPH: 'http://mu.semte.ch/graphs/public'
    links:
      - database:database
```

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
Once a valid resource is provided, a [task:Task](http://redpencil.data.gift/vocabularies/tasks/) is created and processed. It retries in case of failure.
There is also a fallback, in case of issues with notifications

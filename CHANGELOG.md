## 0.6.4 (2025-12-09)

#### :bug: Bug Fix
* Improve logging ([@lagartoverde](https://github.com/lagartoverde))

#### Committers: 1
- Oscar Rodriguez ([@lagartoverde](https://github.com/lagartoverde))

## 0.6.3 (2025-12-08)

#### :bug: Bug Fix
* [#16](https://github.com/lblod/besluit-publicatie-melding-service/pull/16) Task retries were not responding well to 409 errors([@lagartoverde](https://github.com/lagartoverde))

#### Committers: 1
- Oscar Rodriguez ([@lagartoverde](https://github.com/lagartoverde))

## 0.6.2 (2025-10-21)

#### :bug: Bug Fix
* [#14](https://github.com/lblod/besluit-publicatie-melding-service/pull/14) Fix error handling in rescheduleUnproccessedTasks ([@cecemel](https://github.com/cecemel))
* [#15](https://github.com/lblod/besluit-publicatie-melding-service/pull/15) Correctly process the already submitted error from the automatic submission service ([@lagartoverde](https://github.com/lagartoverde))

#### Committers: 1
- Oscar Rodriguez ([@lagartoverde](https://github.com/lagartoverde))

## 0.6.1 (2023-07-07)

#### :bug: Bug Fix
* [#11](https://github.com/lblod/besluit-publicatie-melding-service/pull/11) GN-4373: prevent `undefined` error in `requiresMelding` by checking if query has any results ([@elpoelma](https://github.com/elpoelma))
* [#13](https://github.com/lblod/besluit-publicatie-melding-service/pull/13) Move code between mutex acquire and release in try-catch-block ([@elpoelma](https://github.com/elpoelma))
* [#12](https://github.com/lblod/besluit-publicatie-melding-service/pull/12) GN-4373: update url construction for published resources to reflect gn-publication route changes ([@elpoelma](https://github.com/elpoelma))

#### Committers: 1
- Elena Poelman ([@elpoelma](https://github.com/elpoelma))


## 0.6.0 (2023-01-27)

#### :rocket: Enhancement
* [#8](https://github.com/lblod/besluit-publicatie-melding-service/pull/8) Fetch BesluitTypes from endpoint ([@benjay10](https://github.com/benjay10))
* [#5](https://github.com/lblod/besluit-publicatie-melding-service/pull/5) enhancement: verify publication url returns an ok before submitting ([@nvdk](https://github.com/nvdk))

#### :house: Internal
* [#10](https://github.com/lblod/besluit-publicatie-melding-service/pull/10) Set up release-it ([@elpoelma](https://github.com/elpoelma))

#### Committers: 3
- Ben ([@benjay10](https://github.com/benjay10))
- Elena Poelman ([@elpoelma](https://github.com/elpoelma))
- Niels V ([@nvdk](https://github.com/nvdk))


# bedrock-ledger-consensus-continuity ChangeLog

## 9.0.0 - TBD

### Changed
- Changed `witnesses` from an `Array` to a `Set`.
- **BREAKING:** Renamed `voters` to `peers` in various routes.

### Removed
- Removed `bedrock-rest` from the project.

## 8.0.1 - TBD

### Removed
- `axios` was removed from `lib/client.js` in order to move away from axios.
- `axios` was removed from `package.json`

### Added
- `@digitalbazaar/http-client` was added to the project.
- `lib/client.js` `notifyPeer` API sends an HTTP Signature in each request.
- `lib/server.js` the notify route now verifies HTTP signatures before accepting a notification.

### Changed
- Replaced `axios` with `@digitalbazaar/http-client`
- `lib/client.js` now uses `@digitalbazaar/http-client`.

## 8.0.0 - 2021-04-29

### Changed
- **BREAKING**: Use `bedrock-mongodb` 8.1.x.
- **BREAKING**: Use `bedrock-ledger-storage-mongodb`: ^4.0.0.
- Update numerous mongodb calls to match mongo driver 3.5 api.
- Increment engines.node to >= 12.

### Removed
- **BREAKING**: Dependency on `bedrock-identity`.
- **BREAKING**: Removed `mode` and recovery mode options.
- **BREAKING**: Remove callback-based API (only promises supported).

## 7.0.1 - 2020-10-08

### Fixed
- Fix getHead calculation in getMergeStatus.
- Fix call to notifyFlag API.

## 7.0.0 - 2020-09-30

### Fixed
- **BREAKING**: Ensure first ancestors from other branches are
  included in most recent ancestors calculations.

### Changed
- Use jsonld-signatures@6.
- Use crypto-ld@3.9.0.

## 6.0.0 - 2020-06-18

### Added
- Setup CI workflow.

### Changed
- **BREAKING**: Remove `bedrock-docs` dependency and RAML documentation.
- Use async iterator in getEventsForGossip.

## 5.0.3 - 2020-02-19

### Fixed
- Fix merge permit consumption calculation bug.

## 5.0.2 - 2020-01-03

### Fixed
- **BREAKING** Revert need for a supermajority of proposals at
  a decision. This reverts back to 5.0.0 behavior.

## 5.0.1 - 2019-12-27

### Fixed
- **BREAKING** Ensure a supermajority of proposals are present
  at a decision.

## 5.0.0 - 2019-12-26

### Changed
- Implement "single-pipeline" worker that coordinates the entire
  ledger node (including running the consensus algorithm, gossipping
  with peers, writing events, merging, and sending notifications).
- Implement a number of optimizations, e.g., hashing merge events
  with fewer computations, reduce calls to `getLatestSummary`,
  calculate priority peers based on consensus results.
- Update dependencies, including using the latest web ledger context
  that uses JSON literals for web ledger records.
- Improve client error reporting.

### Fixed
- **BREAKING** Change consensus support algorithm to better prevent
  all byzantine failures. This is a breaking change because it changes
  which events achieve consensus.

## 4.0.1 - 2019-11-13

### Changed
- Update peer dependency for bedrock v1 - v3.

# 4.0.0 - 2019-11-11

### Added

- Support recovery mode capability, i.e., enable networks to require
  `2f+1` support to each consensus but only `2r+1` support to make
  decisions or to trigger new sets of electors to be chosen to ensure
  network health and continuity.
- Support multiple modes for determining which events reach consensus:
  `first`, `firstWithConsensusProof`, and `batch`.
- **BREAKING** Set default consensus mode to `first`.
- Cache `prime` APIs that can be used to prime the redis cache based on data
  stored in MongoDB.

### Fixed
- **BREAKING** Fix bugs related to consensus support
  calculation. This is a bug fix but a breaking change
  because it alters how support is calculated which
  changes which events would achieve consensus.

# 3.7.0 - 2019-09-30

### Changed
- Set default debounce configuration settings to zero. This serves to
  significantly reduce consensus times in exchange for a modest increase in
  CPU utilization.

# 3.6.0 - 2019-09-06

### Changed
- Update deps in support of using Node 12 Ed25519 crypto. All native module
  dependencies are now optional when used with Node 12.

# 3.5.0 - 2019-08-29

### Added
- Implement maxRetries when attempting to process gossip batches. This prevents
  a malformed gossip batch from introducing unnecessary delays in the gossip
  pipeline.

## 3.4.1 - 2019-08-22

### Fixed
- Defer gossip batch processing based on basisBlockHeight. Do not attempt to
  validate events until the local node has generated the block indicated by
  the event's basisBlockHeight.

## 3.4.0 - 2019-05-30

### Changed
- Use bedrock-ledger-context@12.

## 3.3.1 - 2019-05-30

### Fixed
- Ensure that `basisBlockHeight` is defined in `OperationQueue`.
- During gossip, respect server limits by requesting needed events in chunks.
- Fix `hasMore` flag in `events.create` API.
- Use `noopLogger` in consensus worker.


## 3.3.0 - 2019-03-25

### Changed
- Use bedrock-ledger-node@8.

## 3.2.1 - 2019-02-15

### Fixed
- Add genesis merge exception in `_validateGraph`.

## 3.2.0 - 2019-02-13

### Changed
- Update several dependencies. See git history for changes.


## 3.1.0 - 2019-01-23

### Changed
- Use jsonld-signatures 3.x for signing/verifying merge events.

## 3.0.2 - 2019-01-02

### Fixed
- Fix computation of merge `maxEvents`.

## 3.0.1 - 2019-01-01

### Fixed
- Fix conditional related to generation one merge events in `_validateGraph`.

## 3.0.0 - 2018-12-31

### Added
- Implement `basisBlockHeight` to be used for validation of operations.

### Changed
- **BREAKING** Require `sequence` in ledger configurations.
- **BREAKING** Require `creator` in operations and ledger configuration updates.

## 2.0.0 - 2018-09-20

### Changed
- Use bedrock-validation 3.x.

## 1.0.1 - 2018-09-20

### Fixed
- Use the proper `_cacheKey` API.

## 1.0.0 - 2018-09-11

- See git history for changes.

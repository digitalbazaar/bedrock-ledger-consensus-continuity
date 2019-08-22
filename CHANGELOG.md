# bedrock-ledger-consensus-continuity ChangeLog

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

/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;

module.exports =
  bedrock.loggers.get('app').child(config['ledger-continuity'].loggerNamespace);

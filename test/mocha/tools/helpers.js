/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';
const crypto = require('crypto');
const util = require('util');
const randomBytes = util.promisify(crypto.randomBytes);

const api = {};
module.exports = api;

api.strfy = a => JSON.stringify(a, null, 2);

api.generateId = generateId;

async function generateId() {
  const buffer = await randomBytes(8);
  return buffer.toString('base64');
}

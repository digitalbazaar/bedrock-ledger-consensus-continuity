/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports = new Proxy({}, {
  get() {
    return () => {};
  }
});

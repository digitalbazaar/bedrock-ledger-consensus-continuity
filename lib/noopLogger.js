/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports = new Proxy({}, {
  get() {
    return () => {};
  }
});

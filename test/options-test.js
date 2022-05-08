'use strict'

const testCommon = require('./common')
const test = require('tape')

test('cacheSize 0', function (t) {
  db = testCommon.factory({
    cacheSize: 0
  })
  db.open(t.end.bind(t))
})
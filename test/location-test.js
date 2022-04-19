'use strict'

const test = require('tape')
const { RocksLevel } = require('..')

test('test database creation non-string location throws', function (t) {
  t.throws(() => new RocksLevel({}), {
    name: 'TypeError',
    message: "The first argument 'location' must be a non-empty string"
  })
  t.throws(() => new RocksLevel(''), {
    name: 'TypeError',
    message: "The first argument 'location' must be a non-empty string"
  })
  t.end()
})

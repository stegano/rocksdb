'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory()
  db.open(t.end.bind(t))
})

test('test invalid getLatestSequenceNumber() returns big int', function (t) {
  t.equal(typeof db.getLatestSequenceNumber(), 'bigint')
  t.end()
})

test('tearDown', function (t) {
  db.close(t.end.bind(t))
})

test('getLatestSequenceNumber() throws if db is closed', function (t) {
  t.throws(() => db.getLatestSequenceNumber(), {
    code: 'LEVEL_DATABASE_NOT_OPEN'
  })
  t.end()
})

'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory()
  db.open(t.end.bind(t))
})

test('test getUpdatesSince()', function (t) {
  const seq = db.getLatestSequenceNumber()
  db.batch([{ type: 'put', key: 'key', value: 'val' }], (err) => {
    t.error(err)
    db.getUpdateSince({ since: seq }, (err, val, seq) => {
      t.error(err)
      t.equal(seq, 1n)
      t.equal(val[0], 'key')
      t.equal(val[1], 'val')
      t.end()
    })
  })
})

test('tearDown', function (t) {
  db.close(t.end.bind(t))
})

test('getUpdatesSince() throws if db is closed', function (t) {
  t.throws(() => db.getLatestSequenceNumber(), {
    code: 'LEVEL_DATABASE_NOT_OPEN'
  })
  t.end()
})

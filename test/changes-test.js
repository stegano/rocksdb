'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory()
  db.open(t.end.bind(t))
})

test('test changes()', async function (t) {
  await db.batch([{ type: 'put', key: 'should not exit', value: 'val' }])
  await db.batch([{ type: 'put', key: 'key', value: 'val' }])
  const seq = db.getLatestSequenceNumber()
  
  const val = []
  for await (const { updates, sequence } of db.changes({ since: seq })) {
    t.equal(sequence, 2n)
    val.push(...updates)
  }

  t.equal(seq, 2n)
  t.equal(val[0], 'key')
  t.equal(val[1], 'val')
  t.end()
})

test('test changes() bad seq', async function (t) {  
  try {
    for await (const { updates, sequence } of db.changes({ since: 10n })) {
      t.fail()
    }
  } catch (err) {
    t.equals(err.code, 'LEVEL_NOT_FOUND')
  }
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

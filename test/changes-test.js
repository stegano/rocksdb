'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory()
  db.open(t.end.bind(t))
})

test('test updates()', async function (t) {
  await db.batch([{ type: 'put', key: 'should not exit', value: 'val' }])
  await db.batch([{ type: 'put', key: 'key', value: 'val' }])
  const seq = db.getLatestSequenceNumber()

  const val = []
  for await (const { rows, sequence } of db.updates({ since: seq })) {
    t.equal(sequence, 2n)
    val.push(...rows)
  }

  t.equal(seq, 2n)
  t.equal(val[0], 'key')
  t.equal(val[1], 'val')
  t.end()
})

test('test updates() bad seq', async function (t) {
  try {
    for await (const _ of db.updates({ since: 10n })) {
      console.log(_)
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

'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory()
  db.open(t.end.bind(t))
})

test('test updates()', async function (t) {
  const batch1 = db.batch()
  batch1.put('key1', 'val2')
  await batch1.write()

  const batch2 = db.batch()
  batch2.put('key', 'val')
  batch2.putLogData('hello1')
  batch2.putLogData('hello2')
  await batch2.write()

  const val = []
  for await (const { rows, sequence } of db.updates({ since: 2n })) {
    t.equal(sequence, 2)
    val.push(...rows)
  }

  t.equal(val[0], 'put')
  t.equal(val[1], 'key')
  t.equal(val[2], 'val')
  t.equal(val[3], 'data')
  t.equal(val[4], null)
  t.equal(val[5], 'hello1')
  t.equal(val[6], 'data')
  t.equal(val[7], null)
  t.equal(val[8], 'hello2')
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

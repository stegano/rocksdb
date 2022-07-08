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
  const allData = []
  for await (const { rows, sequence, data } of db.updates({ since: 2n })) {
    t.equal(sequence, 2)
    val.push(...rows)
    allData.push(...data)
  }

  t.equal(val[0], 'key')
  t.equal(val[1], 'val')
  t.equal(allData[0], 'hello1')
  t.equal(allData[1], 'hello2')
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

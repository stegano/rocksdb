'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory({
    walSizeLimit: 1e6
  })
  db.open(t.end.bind(t))
})

test('test updates()', async function (t) {
  const batch1 = db.batch()
  const seq1 = db.sequence
  batch1.put('key1', 'val2')
  await batch1.write()
  t.equal(1, db.sequence - seq1)
  t.equal(batch1.count, 1)

  const batch2 = db.batch()
  const seq2 = db.sequence
  batch2.put('key', 'val')
  batch2.putLogData('hello1')
  batch2.putLogData('hello2')
  await batch2.write()
  t.equal(2, db.sequence - seq1)
  t.equal(1, db.sequence - seq2)
  t.equal(batch2.count, 1)

  const val = []
  for await (const { rows, sequence, count } of db.updates({ since: 2 })) {
    t.equal(count, 1)
    t.equal(sequence, 2)
    t.equal(sequence + count - 1, db.sequence)
    val.push(...rows)
  }

  let n = 0
  t.equal(val[n++], 'put')
  t.equal(val[n++], 'key')
  t.equal(val[n++], 'val')
  n++
  t.equal(val[n++], 'data')
  t.equal(val[n++], null)
  t.equal(val[n++], 'hello1')
  n++
  t.equal(val[n++], 'data')
  t.equal(val[n++], null)
  t.equal(val[n++], 'hello2')
  n++
  t.end()
})

test('test updates() bad seq', async function (t) {
  try {
    for await (const _ of db.updates({ since: 10 })) {
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

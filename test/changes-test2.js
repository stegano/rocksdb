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
  batch1.put('key1', 'val1')
  batch1.putLogData('data1')
  batch1.put('key2', 'val2')
  batch1.putLogData('data1')
  batch1.put('key3', 'val3')
  batch1.putLogData('data1')
  await batch1.write()

  let since = 0

  for await (const { rows, sequence, count } of db.updates({ since })) {
    since = sequence + count - 1
  }

  t.equal(since, 3)

  const batch2 = db.batch()
  batch2.put('key', 'val')
  batch2.putLogData('hello1')
  batch2.putLogData('hello2')
  await batch2.write()

  t.equal(db.sequence, 4)

  since += 1

  for await (const { rows, sequence, count } of db.updates({ since, data: true, values: true, keys: true })) {
    t.equal(sequence, since)
    since = sequence + count - 1
    t.same(rows, [
      'put', 'key', 'val', 'default',
      'data', null, 'hello1', null,
      'data', null, 'hello2', null,
    ])
  }

  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory({
    walSizeLimit: 1e6,
    columns: { default: {} }
  })
  db.open(t.end.bind(t))
})

test('test sequences()', async function (t) {
  const batches = []
  db.on('write', (batch, sequence) => {
    batches.push({ rows: [...batch], sequence })
  })

  const batch1 = db.batch()
  batch1._put('key1', 'val1')
  batch1._putLogData('data1')
  batch1._put('key2', 'val2')
  batch1._putLogData('data1')
  batch1._put('key3', 'val3')
  batch1._putLogData('data1')
  await batch1.write()

  const batch2 = db.batch()
  batch2._put('key1', 'val1')
  batch2._putLogData('data1')
  batch2._put('key2', 'val2')
  batch2._putLogData('data1')
  batch2._put('key3', 'val3')
  batch2._putLogData('data1')
  await batch2.write()

  for await (const { sequence } of db.updates({ since: 0, live: false })) {
    t.equals(batches.shift().sequence, sequence)
  }

  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

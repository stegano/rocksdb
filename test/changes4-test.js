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

test('test live()', async function (t) {
  const batch1 = db.batch()
  batch1._put('key1', 'val1')
  batch1._putLogData('data1')
  batch1._put('key2', 'val2')
  batch1._putLogData('data1')
  batch1._put('key3', 'val3')
  batch1._putLogData('data1')

  const batch2 = db.batch()
  batch2._put('key11', 'val11')
  batch2._putLogData('data11')
  batch2._put('key22', 'val22')
  batch2._putLogData('data11')
  batch2._put('key33', 'val33')
  batch2._putLogData('data11')

  const batches = [batch1.toArray(), batch2.toArray()]

  for await (const update of db.updates({ since: 0, live: false })) {
    t.fail(update)
  }

  setTimeout(() => {
    batch1.write()
  }, 1e2)

  setTimeout(() => {
    batch2.write()
  }, 2e2)

  for await (const update of db.updates({ since: 0, live: true })) {
    t.same(update.rows, batches.shift())
    if (batches.length === 0) {
      break
    }
  }

  t.end()
})
test('tearDown', async function (t) {
  await db.close()
  t.end()
})

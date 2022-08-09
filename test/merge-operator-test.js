'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', async function (t) {
  db = testCommon.factory({
    walSizeLimit: 1e6,
    columns: {
      default: {
        mergeOperator: 'maxRev'
      }
    }
  })
  await db.open()
  t.end()
})

test('test merge maxRev()', async function (t) {
  const batch = db.batch()
  batch._merge('key1', '1-asd')
  batch._merge('key1', '3-asd')
  batch._merge('key1', '2-asd')
  await batch.write()

  t.same(await db.get('key1'), '3-asd')

  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

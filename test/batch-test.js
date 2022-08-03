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

test('test batch iterate', async function (t) {
  const batch = db.batch()
  batch.put('1', 'a')
  batch.put('2', 'b')
  t.same([...batch], [
    { type: 'put', key: '1', value: 'a' },
    { type: 'put', key: '2', value: 'b' }
  ])
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

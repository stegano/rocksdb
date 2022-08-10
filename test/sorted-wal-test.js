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

test('test sorted wal', async function (t) {
  const batch = db.batch()
  batch._put('1', 'a')
  batch._put('2', 'b')
  await batch.write()
  const walFiles = await db.getSortedWalFiles()
  t.ok(Array.isArray(walFiles))
  t.ok(walFiles[0])
  t.ok(typeof walFiles[0].pathName === 'string')
  t.ok(typeof walFiles[0].logNumber === 'number')
  t.ok(typeof walFiles[0].type === 'number')
  t.ok(typeof walFiles[0].startSequence === 'number')
  t.ok(typeof walFiles[0].sizeFileBytes === 'number')
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

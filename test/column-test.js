'use strict'

const test = require('tape')
const testCommon = require('./common')

test('test chained-batch', async function (t) {
  const db = testCommon.factory()
  await db.open({
    columns: { test: {}, default: {} }
  })
  const column = db.columns.test

  t.ok(column)

  const batch = db.batch()
  batch.put('foo', 'val1', { column })
  batch.put('bar', 'val2', { column })
  batch.put('foo', 'val3')
  batch.put('bar', 'val4')

  await batch.write()

  t.equal(await db.get('foo', { column }), 'val1')
  t.equal(await db.get('bar', { column }), 'val2')
  t.equal(await db.get('foo'), 'val3')
  t.equal(await db.get('bar'), 'val4')

  await db.close()

  t.end()
})

test('test batch', async function (t) {
  const db = testCommon.factory()
  await db.open({
    columns: { test: {}, default: {} }
  })
  const column = db.columns.test

  t.ok(column)

  await db.batch([{ type: 'put', key: 'foo', value: 'val1', column }])
  await db.batch([{ type: 'put', key: 'bar', value: 'val2', column }])
  await db.batch([{ type: 'put', key: 'foo', value: 'val3' }])
  await db.batch([{ type: 'put', key: 'bar', value: 'val4' }])

  t.equal(await db.get('foo', { column }), 'val1')
  t.equal(await db.get('bar', { column }), 'val2')
  t.equal(await db.get('foo'), 'val3')
  t.equal(await db.get('bar'), 'val4')

  await db.close()

  t.end()
})

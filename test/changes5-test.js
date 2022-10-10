'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory({
    walSizeLimit: 1e6,
    columns: {
      default: {},
      asd: {}
    }
  })
  db.open(t.end.bind(t))
})

test('test updates column filter()', async function (t) {
  const batch1 = db.batch()
  batch1._put('key1', 'val1', { column: db.columns.default })
  batch1._put('key3', 'val3', { column: db.columns.default })
  batch1._put('key1', 'val1', { column: db.columns.asd })
  batch1._put('key2', 'val2', { column: db.columns.asd })
  batch1._put('key4', 'val4', { column: db.columns.default })
  await batch1.write()

  const val = []
  for await (const { rows } of db.updates({
		since: 0,
		column: db.columns.asd,
		keys: true,
		values: true,
		data: true,
	})) {
    val.push(...rows)
  }

  console.error(...val)

  let n = 0
  t.equal(val[n++], 'put')
  t.equal(val[n++], 'key1')
  t.equal(val[n++], 'val1')
  n++
  t.equal(val[n++], 'put')
  t.equal(val[n++], 'key2')
  t.equal(val[n++], 'val2')
  n++
  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

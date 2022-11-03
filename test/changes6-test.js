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
  batch1._put('key1', 'val2')
  await batch1.write()

  for await (const { rows } of db.updates({ since: 0, valueEncoding: 'buffer', keyEncoding: 'buffer' })) {
		for (const row of rows) {
			console.error(row)
			t.ok(Buffer.isBuffer(row))
		}
  }

  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

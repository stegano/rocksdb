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

test('test updates()', async function (t) {
  t.plan(4)

  const batch1 = db.batch()
  batch1._put('key1', 'val1')
  batch1._putLogData('data1')
  batch1._put('key2', 'val2')
  batch1._putLogData('data1')
  batch1._put('key3', 'val3')
  batch1._putLogData('data1')
  await batch1.write()

  let since = 0

  for await (const { sequence, count } of db.updates({ since })) {
    since = sequence + count - 1
  }

  t.equal(since, 3)

  const batch2 = db.batch()
  batch2._put('key', 'val')
  batch2._putLogData('hello1')
  batch2._putLogData('hello2')
  await batch2.write()

  t.equal(db.sequence, 4)

  since += 1

  for await (const { rows, sequence, count } of db.updates({ since: since - 1, data: true, values: true, keys: true })) {
		if (sequence < since) {
			// TODO (fix)
			continue
		}
    t.equal(sequence, since)
    since = sequence + count - 1
    t.same(rows, [
      'put', 'key', 'val', null,
      'data', null, 'hello1', null,
      'data', null, 'hello2', null
    ])
  }
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})

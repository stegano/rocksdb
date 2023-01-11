'use strict'

const test = require('tape')
const testCommon = require('./common')

let db

test('setUp db', function (t) {
  db = testCommon.factory({
    valueEncoding: 'buffer',
    columns: {
      default: {
        mergeOperator: 'maxRev',
      },
    },
  })
  db.open(t.end.bind(t))
})

test('max rev', async function (t) {
  const batch = db._chainedBatch()

  const rec1 =Buffer.from('03-SuJnevw6qTIF1P-SO34j9xEGiJbQq-render')
	rec1[0] = rec1.byteLength - 1
  batch._merge('test', rec1, {
    column: db.columns.default,
  })
  const rec2 =Buffer.from('02-SuJnevw6qTIF1P-SO34j9xEGiJbQq-render213123213')
	rec2[0] = rec2.byteLength - 1
  batch._merge('test', rec2, {
    column: db.columns.default,
  })

  await batch.write()

  t.same(await db.get('test'), rec1)

  t.end()
})

test('tearDown', async function (t) {
  await db.close()
  t.end()
})


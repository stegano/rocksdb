// 'use strict'

// const test = require('tape')
// const testCommon = require('./common')

// test('get-many w/ hwm should always return something', async function (t) {
//   const db = testCommon.factory()
//   await db.open()

//   const keys = []
//   {
//     const batch = db.batch()
//     for (let n = 0; n < 1000; n++) {
//       keys.push(`${n}`)
//       batch.put(`${n}`, '1'.repeat(1e6))
//     }
//     await batch.write({ sync: true })
//   }

//   const res = db._getManySync(keys, {
//     highWaterMarkBytes: 100,
//     timeout: 0
//   })

//   t.same(res.length, 1)

//   await db.close()

//   t.end()
// })

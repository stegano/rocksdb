// 'use strict'

// const test = require('tape')
// const testCommon = require('./common')

// test('iterator w/ timeout', async function (t) {
//   const db = testCommon.factory()
//   await db.open()

//   const keys = []
//   {
//     const batch = db.batch()
//     for (let n = 0; n < 1e6; n++) {
//       keys.push(`${n}`)
//       batch.put(`${n}`, 'lajdnfasdfsdfsadfsadfkjsadnfljksadfnsalkjdfnlkasjdnflkasjdnfklajsdfnjklasdnflkjasdnfkljasdnfkljasdnfkljasdnfkljasndfklasndflkjsanfkladnsfjknasldkfjnsalkjdfnsalkdfjnlksadjnfklasdjnflkasdjfnlaskjdfnlasdkjfnlaksjfnlaskdjfnalskjdfnsalkdjfnsaldkjfnaslkdjfnlkasdfnlaskdfjnlkasjdfnlkas,ndfkljasnfklsajdnflksjdnflkasjnkdfl')
//     }
//     await batch.write()
//   }
//   const it = db._iterator({
//     timeout: 0,
//     keys: true,
//     keyEncoding: 'buffer',
//     values: true,
//     valueEncoding: 'buffer',
//   })

//   const { rows, finished } = it._nextvSync(keys.length + 1)

//   // TODO (fix):
//   // t.ok(rows.length / 2 < keys.length)
//   console.error("###", keys.length, rows.length / 2, finished)

//   it._closeSync()

//   await db.close()

//   t.end()
// })

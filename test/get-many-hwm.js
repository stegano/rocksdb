'use strict'

const test = require('tape')
const testCommon = require('./common')

test('get-many w/ hwm should always return something', async function (t) {
  const db = testCommon.factory()
  await db.open()

  const keys = []
  {
    const batch = db.batch()
    for (let n = 0; n < 4e3; n++) {
      keys.push(`${n}`)
      batch.put(`${n}`, 'lajdnfasdfsdfsadfsadfkjsadnfljksadfnsalkjdfnlkasjdnflkasjdnfklajsdfnjklasdnflkjasdnfkljasdnfkljasdnfkljasdnfkljasndfklasndflkjsanfkladnsfjknasldkfjnsalkjdfnsalkdfjnlksadjnfklasdjnflkasdjfnlaskjdfnlasdkjfnlaksjfnlaskdjfnalskjdfnsalkdjfnsaldkjfnaslkdjfnlkasdfnlaskdfjnlkasjdfnlkas,ndfkljasnfklsajdnflksjdnflkasjnkdfl')
    }
    await batch.write()
  }

  const res = db._getManySync(keys, {
    highWaterMarkBytes: 1
  })

  t.ok(res.length === 1)

  await db.close()

  t.end()
})

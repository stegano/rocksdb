'use strict'

const test = require('tape')
const testCommon = require('./common')

test('get-many w/ timeout', async function (t) {
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

  db._getManySync(keys, {
    timeout: 1
  })

  await db.close()

  t.end()
})

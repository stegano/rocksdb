'use strict'

const test = require('tape')
const { Regex } = require('..')

test('regex.test', (t) => {
  t.ok(new Regex('.*').test(Buffer.from('asd:user')))
  t.ok(new Regex(':user').test(Buffer.from('asd:user')))
  t.ok(!new Regex(':user$').test(Buffer.from('asd:user123')))
  t.ok(!new Regex('^(?:({.*}):)?(render\.(stats|methods|result|description|query|args|status|opts))(?:[?].*)?$').test(Buffer.from('asd:user123')))
  t.end()
})

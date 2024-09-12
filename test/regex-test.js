'use strict'

const test = require('tape')
const { Regex } = require('..')

test('regex.test', (t) => {
  t.ok(new Regex('.*').test(Buffer.from('asd:user')))
  t.ok(new Regex(':user').test(Buffer.from('asd:user')))
  t.ok(!new Regex(':user$').test(Buffer.from('asd:user123')))
  t.end()
})

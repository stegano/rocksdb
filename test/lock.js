'use strict'

const { RocksLevel } = require('..')

const location = process.argv[2]
const db = new RocksLevel(location)

db.open(function (err) {
  process.send(err)
})

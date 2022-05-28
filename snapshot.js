'use strict'

const binding = require('./binding')

const kDbContext = Symbol('db')
const kContext = Symbol('context')

class Snapshot {
  constructor (db, context) {
    this[kDbContext] = context
    this[kContext] = binding.snapshot_init(context)
  }

  get sequenceNumber () {
    return binding.snapshot_get_sequence_number(this[kContext])
  }

  close () {
    binding.snapshot_close(this[kContext])
  }
}

exports.Snapshot = Snapshot

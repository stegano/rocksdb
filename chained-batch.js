'use strict'

const { AbstractChainedBatch } = require('abstract-level')
const binding = require('./binding')

const kDbContext = Symbol('db')
const kBatchContext = Symbol('context')

class ChainedBatch extends AbstractChainedBatch {
  constructor (db, context) {
    super(db)

    this[kDbContext] = context
    this[kBatchContext] = binding.batch_init(context)
  }

  _put (key, value) {
    binding.batch_put(this[kBatchContext], key, value)
  }

  _del (key) {
    binding.batch_del(this[kBatchContext], key)
  }

  _clear () {
    binding.batch_clear(this[kBatchContext])
  }

  _write (options, callback) {
    binding.batch_write(this[kDbContext], this[kBatchContext], options, callback)
  }
}

exports.ChainedBatch = ChainedBatch

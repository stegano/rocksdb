'use strict'

const { AbstractChainedBatch } = require('abstract-level')
const binding = require('./binding')

const kDbContext = Symbol('db')
const kBatchContext = Symbol('context')
const kHasData = Symbol('hasData')

class ChainedBatch extends AbstractChainedBatch {
  constructor (db, context) {
    super(db)

    this[kDbContext] = context
    this[kBatchContext] = binding.batch_init(context)
    this[kHasData] = false
  }

  _put (key, value) {
    binding.batch_put(this[kBatchContext], key, value)
    this[kHasData] = true
  }

  _del (key) {
    binding.batch_del(this[kBatchContext], key)
    this[kHasData] = true
  }

  _clear () {
    if (this[kHasData]) {
      binding.batch_clear(this[kBatchContext])
      this[kHasData] = false
    }
  }

  _write (options, callback) {
    if (this[kHasData]) {
      binding.batch_write(this[kDbContext], this[kBatchContext], options, callback)
    } else {
      process.nextTick(callback)
    }
  }
}

exports.ChainedBatch = ChainedBatch

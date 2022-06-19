'use strict'

const { AbstractChainedBatch } = require('abstract-level')
const binding = require('./binding')

const kDbContext = Symbol('db')
const kBatchContext = Symbol('context')

class ChainedBatch extends AbstractChainedBatch {
  constructor (db, context) {
    super(db)

    this[kDbContext] = context
    this[kBatchContext] = binding.batch_init(this[kDbContext])
  }

  _put (key, value, options) {
    // TODO: Error?
    binding.batch_put(this[kDbContext], this[kBatchContext], key, value, options)
  }

  _del (key, options) {
    // TODO: Error?
    binding.batch_del(this[kDbContext], this[kBatchContext], key, options)
  }

  _clear () {
    binding.batch_clear(this[kDbContext], this[kBatchContext])
  }

  _write (options, callback) {
    process.nextTick(callback, binding.batch_write(this[kDbContext], this[kBatchContext], options))
  }

  _close (callback) {
    process.nextTick(callback)
  }
}

exports.ChainedBatch = ChainedBatch

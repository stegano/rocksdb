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
    binding.batch_put(this[kDbContext], this[kBatchContext], key, value, options)
  }

  _del (key, options) {
    binding.batch_del(this[kDbContext], this[kBatchContext], key, options)
  }

  _clear () {
    binding.batch_clear(this[kDbContext], this[kBatchContext])
  }

  _write (options, callback) {
    try {
      binding.batch_write(this[kDbContext], this[kBatchContext], options)
      process.nextTick(callback, null)
    } catch (err) {
      process.nextTick(callback, err)
    }
  }

  _close (callback) {
    process.nextTick(callback)
  }

  putLogData (data, options) {
    // TODO (fix): Check if open...
    binding.batch_put_log_data(this[kDbContext], this[kBatchContext], data, options)
  }

  merge (key, value, options = {}) {
    // TODO (fix): Check if open...
    binding.batch_merge(this[kDbContext], this[kBatchContext], key, value, options)
  }

  get count () {
    return binding.batch_count(this[kDbContext], this[kBatchContext])
  }

  * [Symbol.iterator] () {
    const rows = binding.batch_iterate(this[kBatchContext], {
      keys: true,
      values: true,
      data: true
    })
    for (let n = 0; n < rows.length; n += 4) {
      yield {
        type: rows[n + 0],
        key: rows[n + 1],
        value: rows[n + 2]
      }
    }
  }
}

exports.ChainedBatch = ChainedBatch

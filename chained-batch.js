'use strict'

const { AbstractChainedBatch } = require('abstract-level')
const binding = require('./binding')

const kWrite = Symbol('write')
const kBatchContext = Symbol('batchContext')
const kDbContext = Symbol('dbContext')

class ChainedBatch extends AbstractChainedBatch {
  constructor (db, context, write) {
    super(db)

    this[kWrite] = write
    this[kDbContext] = context
    this[kBatchContext] = binding.batch_init()
  }

  _put (key, value, options) {
    binding.batch_put(this[kBatchContext], key, value, options ?? {})
  }

  _del (key, options) {
    binding.batch_del(this[kBatchContext], key, options ?? {})
  }

  _clear () {
    binding.batch_clear(this[kBatchContext])
  }

  _write (options, callback) {
    this[kWrite](this, this[kBatchContext], options ?? {}, callback)
  }

  _close (callback) {
    process.nextTick(callback)
  }

  get length () {
    return binding.batch_count(this[kBatchContext])
  }

  _putLogData (data, options) {
    binding.batch_put_log_data(this[kBatchContext], data, options ?? {})
  }

  _merge (key, value, options) {
    binding.batch_merge(this[kBatchContext], key, value, options ?? {})
  }

  * [Symbol.iterator] () {
    const rows = binding.batch_iterate(this[kDbContext], this[kBatchContext], {
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

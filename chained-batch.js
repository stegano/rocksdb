'use strict'

const { AbstractChainedBatch } = require('abstract-level')
const binding = require('./binding')

const kWrite = Symbol('write')
const kBatchContext = Symbol('context')

class ChainedBatch extends AbstractChainedBatch {
  constructor (db, write) {
    super(db)

    this[kWrite] = write
    this[kBatchContext] = binding.batch_init()
  }

  _put (key, value, options) {
    binding.batch_put(this[kBatchContext], key, value, options)
  }

  _del (key, options) {
    binding.batch_del(this[kBatchContext], key, options)
  }

  _clear () {
    binding.batch_clear(this[kBatchContext])
  }

  _write (options, callback) {
    this[kWrite](this, this[kBatchContext], options, callback)
  }

  _close (callback) {
    process.nextTick(callback)
  }

  putLogData (data, options) {
    // TODO (fix): Check if open...
    binding.batch_put_log_data(this[kBatchContext], data, options)
  }

  merge (key, value, options = {}) {
    // TODO (fix): Check if open...
    binding.batch_merge(this[kBatchContext], key, value, options)
  }

  get count () {
    return binding.batch_count(this[kBatchContext])
  }

  forEach (fn, options) {
    const rows = binding.batch_iterate(this[kBatchContext], {
      keys: true,
      values: true,
      data: true,
      ...options
    })
    for (let n = 0; n < rows.length; n += 4) {
      fn({
        type: rows[n + 0],
        key: rows[n + 1],
        value: rows[n + 2]
      }, n, this)
    }
  }

  toArray (options) {
    const result = []
    this.forEach(val => {
      result.push(val)
    }, options)
    return result
  }
}

exports.ChainedBatch = ChainedBatch

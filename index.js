'use strict'

const { AbstractLevel } = require('abstract-level')
const ModuleError = require('module-error')
const fs = require('fs')
const binding = require('./binding')
const { ChainedBatch } = require('./chained-batch')
const { Iterator } = require('./iterator')

const kContext = Symbol('context')
const kLocation = Symbol('location')

class ClassicLevel extends AbstractLevel {
  constructor (location, options, _) {
    // To help migrating to abstract-level
    if (typeof options === 'function' || typeof _ === 'function') {
      throw new ModuleError('The levelup-style callback argument has been removed', {
        code: 'LEVEL_LEGACY'
      })
    }

    if (typeof location !== 'string' || location === '') {
      throw new TypeError("The first argument 'location' must be a non-empty string")
    }

    super({
      encodings: {
        buffer: true,
        utf8: true
      },
      seek: true,
      createIfMissing: true,
      errorIfExists: true
    }, options)

    this[kLocation] = location
    this[kContext] = binding.db_init()
  }

  get location () {
    return this[kLocation]
  }

  _open (options, callback) {
    if (options.createIfMissing) {
      fs.mkdir(this[kLocation], { recursive: true }, (err) => {
        if (err) return callback(err)
        binding.db_open(this[kContext], this[kLocation], options, callback)
      })
    } else {
      binding.db_open(this[kContext], this[kLocation], options, callback)
    }
  }

  _close (callback) {
    binding.db_close(this[kContext], callback)
  }

  _put (key, value, options, callback) {
    binding.db_put(this[kContext], key, value, options, callback)
  }

  _get (key, options, callback) {
    binding.db_get(this[kContext], key, options, callback)
  }

  _getMany (keys, options, callback) {
    binding.db_get_many(this[kContext], keys, options, callback)
  }

  _del (key, options, callback) {
    binding.db_del(this[kContext], key, options, callback)
  }

  _clear (options, callback) {
    binding.db_clear(this[kContext], options, callback)
  }

  _chainedBatch () {
    return new ChainedBatch(this, this[kContext])
  }

  _batch (operations, options, callback) {
    binding.batch_do(this[kContext], operations, options, callback)
  }

  _iterator (options) {
    return new Iterator(this, this[kContext], options)
  }
}

exports.RocksLevel = RocksLevel

'use strict'

const { fromCallback } = require('catering')
const { AbstractLevel } = require('abstract-level')
const ModuleError = require('module-error')
const fs = require('fs')
const binding = require('./binding')
const { ChainedBatch } = require('./chained-batch')
const { Iterator } = require('./iterator')
const os = require('os')

const kContext = Symbol('context')
const kColumns = Symbol('columns')
const kLocation = Symbol('location')
const kPromise = Symbol('promise')
const kRef = Symbol('ref')
const kUnref = Symbol('unref')
const kRefs = Symbol('refs')
const kPendingClose = Symbol('pendingClose')

const EMPTY = {}

class RocksLevel extends AbstractLevel {
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

    options = {
      ...options, // TODO (fix): Other defaults...
      parallelism: options?.parallelism ?? Math.max(1, os.cpus().length / 2),
      createIfMissing: options?.createIfMissing ?? true,
      errorIfExists: options?.errorIfExists ?? false,
      walTTL: options?.walTTL ?? 0,
      walSizeLimit: options?.walSizeLimit ?? 0,
      walCompression: options?.walCompression ?? false,
      unorderedWrite: options?.unorderedWrite ?? false,
      manualWalFlush: options?.manualWalFlush ?? false,
      walTotalSizeLimit: options?.walTotalSizeLimit ?? 0,
      infoLogLevel: options?.infoLogLevel ?? ''
    }

    // TODO (fix): Check options.

    super({
      encodings: {
        buffer: true,
        utf8: true
      },
      seek: true,
      createIfMissing: true,
      errorIfExists: true,
      additionalMethods: {
        updates: true,
        query: true
      }
    }, options)

    this[kLocation] = location
    this[kContext] = binding.db_init()
    this[kColumns] = {}

    this[kRefs] = 0
    this[kPendingClose] = null

    // .updates(...) uses 'update' listener.
    this.setMaxListeners(100)
  }

  get sequence () {
    return binding.db_get_latest_sequence(this[kContext])
  }

  get location () {
    return this[kLocation]
  }

  get columns () {
    return this[kColumns]
  }

  _open (options, callback) {
    const onOpen = (err, columns) => {
      if (err) {
        callback(err)
      } else {
        this[kColumns] = columns
        callback(null)
      }
    }
    if (options.createIfMissing) {
      fs.mkdir(this[kLocation], { recursive: true }, (err) => {
        if (err) return callback(err)
        binding.db_open(this[kContext], this[kLocation], options, onOpen)
      })
    } else {
      binding.db_open(this[kContext], this[kLocation], options, onOpen)
    }
  }

  [kRef] () {
    this[kRefs]++
  }

  [kUnref] () {
    this[kRefs]--
    if (this[kRefs] === 0 && this[kPendingClose]) {
      process.nextTick(this[kPendingClose])
    }
  }

  _close (callback) {
    if (this[kRefs]) {
      this[kPendingClose] = callback
    } else {
      binding.db_close(this[kContext], callback)
    }
  }

  _put (key, value, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      const batch = this.batch()
      batch.put(key, value, options ?? EMPTY)
      batch.write(callback)
    } catch (err) {
      process.nextTick(callback, err)
    }

    return callback[kPromise]
  }

  _get (key, options, callback) {
    this._getMany([key], options ?? EMPTY, (err, val) => {
      if (err) {
        callback(err)
      } else if (val[0] === undefined) {
        callback(Object.assign(new Error('not found'), {
          code: 'LEVEL_NOT_FOUND'
        }))
      } else {
        callback(null, val[0])
      }
    })
  }

  _getMany (keys, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      this[kRef]()
      binding.db_get_many(this[kContext], keys, options ?? EMPTY, (err, val) => {
        callback(err, val)
        this[kUnref]()
      })
    } catch (err) {
      process.nextTick(callback, err)
      this[kUnref]()
    }

    return callback[kPromise]
  }

  _getMergeOperands (key, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      this[kRef]()
      binding.db_get_merge_operands(this[kContext], key, options ?? EMPTY, (err, val) => {
        callback(err, val)
        this[kUnref]()
      })
    } catch (err) {
      process.nextTick(callback, err)
      this[kUnref]()
    }

    return callback[kPromise]
  }

  _del (key, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      const batch = this.batch()
      batch.del(key, options ?? EMPTY)
      batch.write(callback)
    } catch (err) {
      process.nextTick(callback, err)
    }

    return callback[kPromise]
  }

  _clear (options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      // TODO (fix): Use batch + DeleteRange...
      binding.db_clear(this[kContext], options ?? EMPTY)
      process.nextTick(callback, null)
    } catch (err) {
      process.nextTick(callback, err)
    }

    return callback[kPromise]
  }

  _chainedBatch () {
    return new ChainedBatch(this, this[kContext], (batch, context, options, callback) => {
      try {
        this[kRef]()
        binding.batch_write(this[kContext], context, options, (err, sequence) => {
          this[kUnref]()
          callback(err)
        })
      } catch (err) {
        process.nextTick(callback, err)
      }
    })
  }

  _batch (operations, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      this[kRef]()
      binding.batch_do(this[kContext], operations, options ?? EMPTY, (err, sequence) => {
        this[kUnref]()
        callback(err)
      })
    } catch (err) {
      process.nextTick(callback, err)
    }

    return callback[kPromise]
  }

  _iterator (options) {
    return new Iterator(this, this[kContext], options ?? EMPTY)
  }

  get identity () {
    return binding.db_get_identity(this[kContext])
  }

  getProperty (property) {
    if (typeof property !== 'string') {
      throw new TypeError("The first argument 'property' must be a string")
    }

    // Is synchronous, so can't be deferred
    if (this.status !== 'open') {
      throw new ModuleError('Database is not open', {
        code: 'LEVEL_DATABASE_NOT_OPEN'
      })
    }

    return binding.db_get_property(this[kContext], property)
  }

  async query (options) {
    if (this.status !== 'open') {
      throw new ModuleError('Database is not open', {
        code: 'LEVEL_DATABASE_NOT_OPEN'
      })
    }

    const context = binding.iterator_init(this[kContext], options ?? {})
    try {
      this[kRef]()
      const limit = options.limit ?? 1000
      return await new Promise((resolve, reject) => binding.iterator_nextv(context, limit, (err, rows, finished) => {
        if (err) {
          reject(err)
        } else {
          resolve({
            rows,
            sequence: binding.iterator_get_sequence(context),
            finished
          })
        }
      }))
    } finally {
      binding.iterator_close(context)
      this[kUnref]()
    }
  }

  async flushWal (options) {
    return new Promise((resolve, reject) => {
      binding.db_flush_wal(this[kContext], options ?? {}, (err, val) => err ? reject(err) : resolve(val))
    })
  }
}

exports.RocksLevel = RocksLevel

'use strict'

const { fromCallback } = require('catering')
const { AbortError } = require('./common')
const { AbstractLevel } = require('abstract-level')
const ModuleError = require('module-error')
const fs = require('fs')
const binding = require('./binding')
const { ChainedBatch } = require('./chained-batch')
const { Iterator } = require('./iterator')
const { Readable } = require('readable-stream')
const os = require('os')
const AbortController = require('abort-controller')

const kContext = Symbol('context')
const kColumns = Symbol('columns')
const kLocation = Symbol('location')
const kPromise = Symbol('promise')
const kUpdates = Symbol('updates')
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
      this[kPendingClose]()
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
        const seq = this.sequence
        binding.batch_write(this[kContext], context, options)
        this.emit('update', {
          rows: batch.toArray(),
          count: batch.length,
          sequence: seq + 1
        })
        process.nextTick(callback, null)
      } catch (err) {
        process.nextTick(callback, err)
      }
    })
  }

  _batch (operations, options, callback) {
    callback = fromCallback(callback, kPromise)

    try {
      binding.batch_do(this[kContext], operations, options ?? EMPTY)
      process.nextTick(callback, null)
    } catch (err) {
      process.nextTick(callback, err)
    }

    return callback[kPromise]
  }

  _iterator (options) {
    return new Iterator(this, this[kContext], options ?? EMPTY)
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

  async * updates (options) {
    // TODO (fix): Ensure open status etc...?
    if (this.status !== 'open') {
      throw new Error('Database is not open')
    }

    if (this.status !== 'open') {
      throw new ModuleError('Database is not open', {
        code: 'LEVEL_DATABASE_NOT_OPEN'
      })
    }

    options = {
      since: options?.since ?? 0,
      keys: options?.keys ?? true,
      values: options?.values ?? true,
      data: options?.data ?? true,
      live: options?.live ?? false,
      column: options?.column ?? null,
      signal: options?.signal ?? null
    }

    if (typeof options.since !== 'number') {
      throw new TypeError("'since' must be nully or a number")
    }

    if (typeof options.keys !== 'boolean') {
      throw new TypeError("'keys' must be nully or a boolean")
    }

    if (typeof options.values !== 'boolean') {
      throw new TypeError("'values' must be nully or a boolean")
    }

    if (typeof options.data !== 'boolean') {
      throw new TypeError("'data' must be nully or a boolean")
    }

    if (options.column !== undefined && typeof options.column !== 'object') {
      throw new TypeError("'column' must be nully or a object")
    }

    if (typeof options.live !== 'boolean') {
      throw new TypeError("'live' must be nully or a boolean")
    }

    const ac = new AbortController()
    const onAbort = () => {
      ac.abort()
    }

    options.signal?.addEventListener('abort', onAbort)
    this.on('closing', onAbort)

    const db = this

    try {
      let since = options.since
      while (true) {
        const buffer = new Readable({
          signal: ac.signal,
          objectMode: true,
          readableHighWaterMark: 1024,
          construct (callback) {
            this._next = (update) => {
              if (!this.push(update)) {
                this.push(null)
                db.off('update', this._next)
              }
            }
            db.on('update', this._next)
            callback()
          },
          read () {},
          destroy (err, callback) {
            db.off('update', this._next)
            callback(err)
          }
        })

        if (ac.signal.aborted) {
          throw new AbortError()
        }

        try {
          if (since <= db.sequence) {
            let first = true
            for await (const update of db[kUpdates]({
              ...options,
              signal: ac.signal,
              // HACK: https://github.com/facebook/rocksdb/issues/10476
              since: Math.max(0, options.since)
            })) {
              if (first) {
                if (update.sequence > since) {
                  db.emit('warning', `Invalid updates sequence ${update.sequence} > ${options.since}.`)
                }
                first = false
              }

              if (update.sequence >= since) {
                yield update
                since = update.sequence + update.count
              }
            }
          }
        } catch (err) {
          if (err.code !== 'LEVEL_TRYAGAIN') {
            throw err
          }
        }

        if (!options.live) {
          return
        }

        let first = true
        for await (const update of buffer) {
          if (first) {
            if (update.sequence > since) {
              db.emit('warning', `Invalid batch sequence ${update.sequence} > ${options.since}.`)
            }
            first = false
          }
          if (update.sequence >= since) {
            yield update
            since = update.sequence + update.count
          }
        }
      }
    } finally {
      this.off('closing', onAbort)
      options.signal?.removeEventListener('abort', onAbort)
    }
  }

  async * [kUpdates] ({ signal, ...options }) {
    const context = binding.updates_init(this[kContext], options)
    this[kRef]()
    try {
      while (true) {
        if (signal?.aborted) {
          throw new AbortError()
        }

        const entry = await new Promise((resolve, reject) => binding.updates_next(context, (err, rows, sequence, count) => {
          if (err) {
            reject(err)
          } else {
            resolve({ rows, sequence, count })
          }
        }))

        if (!entry.rows) {
          return
        }

        yield entry
      }
    } finally {
      binding.updates_close(context)
      this[kUnref]()
    }
  }

  async getSortedWalFiles () {
    return new Promise((resolve, reject) => {
      binding.db_get_sorted_wal_files(this[kContext], (err, val) => err ? reject(err) : resolve(val))
    })
  }
}

exports.RocksLevel = RocksLevel

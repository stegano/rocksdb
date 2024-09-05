'use strict'

const { fromCallback } = require('catering')
const { AbstractIterator } = require('abstract-level')

const binding = require('./binding')

const kPromise = Symbol('promise')
const kContext = Symbol('context')
const kCache = Symbol('cache')
const kFinished = Symbol('finished')
const kFirst = Symbol('first')
const kPosition = Symbol('position')
const kEmpty = []

class Iterator extends AbstractIterator {
  constructor (db, context, options) {
    super(db, options)

    this[kContext] = binding.iterator_init(context, options)

    this[kFirst] = true
    this[kCache] = kEmpty
    this[kFinished] = false
    this[kPosition] = 0
  }

  _seek (target) {
    if (target.length === 0) {
      throw new Error('cannot seek() to an empty target')
    }

    this[kFirst] = true
    this[kCache] = kEmpty
    this[kFinished] = false
    this[kPosition] = 0

    binding.iterator_seek(this[kContext], target)
  }

  _next (callback) {
    if (this[kPosition] < this[kCache].length) {
      const key = this[kCache][this[kPosition]++]
      const val = this[kCache][this[kPosition]++]
      process.nextTick(callback, null, key, val)
    } else if (this[kFinished]) {
      process.nextTick(callback)
    } else {
      const size = this[kFirst] ? 1 : 1000
      this[kFirst] = false

      try {
        const { rows, finished } = binding.iterator_nextv(this[kContext], size)
        this[kCache] = rows
        this[kFinished] = finished
        this[kPosition] = 0

        setImmediate(() => this._next(callback))
      } catch (err) {
        process.nextTick(callback, err)
      }
    }

    return this
  }

  _nextv (size, options, callback) {
    callback = fromCallback(callback, kPromise)

    if (this[kFinished]) {
      process.nextTick(callback, null, [])
    } else {
      this[kFirst] = false

      setImmediate(() => {
        try {
          const { rows, finished } = binding.iterator_nextv(this[kContext], size)

          const entries = []
          for (let n = 0; n < rows.length; n += 2) {
            entries.push([rows[n + 0], rows[n + 1]])
          }

          this[kFinished] = finished

          callback(null, entries, finished)
        } catch (err) {
          callback(err)
        }
      })
    }

    return callback[kPromise]
  }

  _nextvSync (size, options) {
    if (this[kFinished]) {
      return []
    }

    const { rows, finished } =  binding.iterator_nextv(this[kContext], size)

    this[kFirst] = false
    this[kFinished] = finished

    return rows
  }

  _close (callback) {
    this[kCache] = kEmpty

    try {
      binding.iterator_close(this[kContext])
      process.nextTick(callback)
    } catch (err) {
      process.nextTick(callback, err)
    }
  }

  _end (callback) {
    this._close(callback)
  }

  // Undocumented, exposed for tests only
  get cached () {
    return (this[kCache].length - this[kPosition]) / 2
  }
}

exports.Iterator = Iterator

'use strict'

const test = require('tape')
const tempy = require('tempy')
const { RocksLevel } = require('..')
const suite = require('abstract-level/test')

module.exports = suite.common({
  test,
  factory (options) {
    const location = tempy.directory()
    return new RocksLevel(location, options)
  }
})

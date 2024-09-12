'use strict'

const binding = require('./binding')

class Regex {
  #context

  constructor (pattern) {
    this.#context = binding.regex_init(pattern)
  }

  test (value) {
    return binding.regex_test(this.#context, value)
  }

  testMany (values) {
    return binding.regex_test_many(this.#context, values)
  }
}

exports.Regex = Regex

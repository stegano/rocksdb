'use strict'

function handleNextv (err, sizes, buffer, finished, options, callback) {
  if (err) {
    callback(err)
  } else {
    buffer ??= Buffer.alloc(0)

    const { keyEncoding, valueEncoding } = options ?? {}

    const rows = []
    let offset = 0
    for (let n = 0; n < sizes.length; n++) {
      const size = sizes[n]
      const encoding = n & 1 ? valueEncoding : keyEncoding
      if (size == null) {
        rows.push(undefined)
      } else {
        if (!encoding || encoding === 'buffer') {
          rows.push(buffer.subarray(offset, offset + size))
        } else if (encoding === 'slice') {
          rows.push({ buffer, byteOffset: offset, byteLength: size })
        } else {
          rows.push(buffer.toString(encoding, offset, offset + size))
        }
        offset += size
        if (offset & 0x7) {
          offset = (offset | 0x7) + 1
        }
      }
    }

    callback(null, rows, finished)
  }
}

exports.handleNextv = handleNextv

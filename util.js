'use strict'

function handleNextv (err, sizes, buffer, finished, options, callback) {
  const { keyEncoding, valueEncoding } = options ?? {}

  if (err) {
    callback(err)
  } else {
    buffer ??= Buffer.alloc(0)
    sizes ??= Buffer.alloc(0)

    const rows = []
    let offset = 0
    const sizes32 = new Int32Array(sizes.buffer, sizes.byteOffset, sizes.byteLength / 4)
    for (let n = 0; n < sizes32.length; n++) {
      const size = sizes32[n]
      const encoding = n & 1 ? valueEncoding : keyEncoding
      if (size < 0) {
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

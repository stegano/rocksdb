import { bench, run, group } from 'mitata'
import { RocksLevel } from '../index.js'

const db = new RocksLevel('./tmp', {
  keyEncoding: 'buffer',
  valueEncoding: 'buffer',
  parallelism: 4,
  pipelinedWrite: false,
  // unorderedWrite: true,
  columns: {
    default: {
      cacheSize: 128e6,
      memtableMemoryBudget: 128e6,
      compaction: 'level'
      // optimize: 'point-lookup',
    }
  }
})
await db.open()

const getOpts = {
  valueEncoding: 'buffer',
  fillCache: true
}

const getUnsafeOpts = {
  valueEncoding: 'buffer',
  fillCache: true,
  unsafe: true
}

for (let size = 1024; size <= 256 * 1024; size *= 2) {
  const keys = []
  for (let n = 0; n < 1024; n++) {
    const key = `${n}-${size}`
    keys.push(Buffer.from(key))
    await db.put(key, Buffer.allocUnsafe(size))
  }
  db._getManySync(keys, getOpts)

  group(() => {
    bench('_getManySync ' + size / 1024, () => {
      db._getManySync(keys, getOpts)
    })

    bench('_getManySync ' + size / 1024 + ' unsafe', () => {
      db._getManySync(keys, getUnsafeOpts)
    })
  })
}

await run()

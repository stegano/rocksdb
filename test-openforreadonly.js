const rocksDB = require("./index.js");

async function testOpenForReadOnly() {
  console.log("Testing OpenForReadOnly...");

  await new Promise((resolve, reject) => {
    setTimeout(() => {
      const db = new rocksDB.RocksLevel("./test-db");
      db.openForReadOnly(
        {
          columns: {
            default: {
              maxOpenFiles: 1,
              cacheSize: 1024 * 1024 * 4,
              writeBufferSize: 1024 * 1024 * 4,
              cacheIndexAndFilterBlocks: true,
            },
            data: {
              maxOpenFiles: 1,
              cacheSize: 1024 * 1024 * 4,
              writeBufferSize: 1024 * 1024 * 4,
              cacheIndexAndFilterBlocks: true,
            },
          },
        },
        (err) => {
          if (err) {
            console.error("OpenForReadOnly error:", err);
            reject(err);
            return;
          }
          console.log("OpenForReadOnly success!");
          console.log("DB status:", db.status);
          db.close(() => {
            resolve();
          });
        }
      );
    }, 1);
  });
}

// 여러 번 테스트 실행
async function runMultipleTests() {
  for (let i = 0; i < 5; i++) {
    console.log(`\n=== Test ${i + 1} ===`);
    await testOpenForReadOnly();
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

runMultipleTests().catch(console.error);

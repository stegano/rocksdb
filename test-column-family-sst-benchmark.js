const { RocksLevel, SstFileWriter } = require("./index.js");
const fs = require("fs");

async function benchmarkSstIngest() {
  const testDir = "./test-bench-db";
  const sstDir = "./test-bench-sst";
  const sstFile = `${sstDir}/users_1k.sst`;
  const NUM = 100000000; // 1억개로 테스트
  const DIRECT_COUNT = 1000; // 직접 DB에 입력할 개수
  const BATCH_SIZE = 100000; // 10만개씩 배치 처리

  // 0. 기존 DB 완전 삭제
  if (fs.existsSync(testDir)) {
    fs.rmSync(testDir, { recursive: true, force: true });
  }

  if (!fs.existsSync(testDir)) fs.mkdirSync(testDir, { recursive: true });
  if (!fs.existsSync(sstDir)) fs.mkdirSync(sstDir, { recursive: true });

  // 1. 컬럼 패밀리 DB 생성
  const db = new RocksLevel(testDir, {
    createIfMissing: true,
    errorIfExists: false,
    columnFamilies: [
      { name: "default" },
      { name: "users" },
      { name: "products" },
    ],
  });

  await db.open();

  // 2. 순차 키 생성 함수
  function generateSequentialKeys(start, count) {
    const keys = [];
    for (let i = 0; i < count; i++) {
      keys.push((start + i).toString().padStart(10, "0"));
    }
    return keys;
  }

  // 3. 직접 DB 데이터 넣기 (1,000건만)
  console.log(`직접 DB에 ${DIRECT_COUNT}개 데이터 입력 시작...`);
  const startDirectWrite = Date.now();
  for (
    let batchStart = 0;
    batchStart < DIRECT_COUNT;
    batchStart += BATCH_SIZE
  ) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, DIRECT_COUNT);
    const batchSize = batchEnd - batchStart;
    for (const key of generateSequentialKeys(batchStart + 1, batchSize)) {
      const value = `direct_value${key}`;
      await db.put(`direct:${key}`, value);
    }
    if (global.gc) global.gc();
  }
  const endDirectWrite = Date.now();
  console.log(
    `직접 DB 데이터 넣기 완료! 소요 시간: ${(
      (endDirectWrite - startDirectWrite) /
      1000
    ).toFixed(2)}초`
  );

  // 4. SstFileWriter로 SST 파일 생성 (각 컬럼 패밀리별로)
  const sstCount = NUM - DIRECT_COUNT;
  console.log(`각 컬럼 패밀리별 SST 파일 생성 시작...`);
  const startWrite = Date.now();

  // 각 컬럼 패밀리별로 별도 SST 파일 생성
  const columnFamilies = ["users", "products"];
  const sstFiles = [];

  for (const cfName of columnFamilies) {
    const cfSstFile = `${sstDir}/${cfName}_data.sst`;
    const cfSstCount = Math.floor(sstCount / columnFamilies.length);

    console.log(`${cfName} 컬럼 패밀리 SST 파일 생성 중...`);
    const writer = new SstFileWriter({
      createIfMissing: true,
      blockSize: 4096,
    });
    writer.openSync(cfSstFile);

    for (
      let batchStart = 0;
      batchStart < cfSstCount;
      batchStart += BATCH_SIZE
    ) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, cfSstCount);
      const batchSize = batchEnd - batchStart;
      for (const key of generateSequentialKeys(
        batchStart +
          1 +
          DIRECT_COUNT +
          columnFamilies.indexOf(cfName) * cfSstCount,
        batchSize
      )) {
        const value = `${cfName}_value${key}`;
        writer.putSync(`${cfName}:${key}`, value);
      }
      if (global.gc) global.gc();
    }

    const fileInfo = writer.finishSync();
    writer.close();
    sstFiles.push({ cfName, filePath: cfSstFile, fileInfo });

    console.log(
      `${cfName} SST 파일 생성 완료! 파일 크기: ${(
        fileInfo.fileSize /
        1024 /
        1024
      ).toFixed(2)}MB`
    );
  }

  const endWrite = Date.now();
  console.log(
    `모든 컬럼 패밀리 SST 파일 생성 완료! 소요 시간: ${(
      (endWrite - startWrite) /
      1000
    ).toFixed(2)}초`
  );

  // 5. 각 컬럼 패밀리에 SST 파일 ingest
  console.log("각 컬럼 패밀리에 SST 파일 ingest 시작...");
  const startIngest = Date.now();

  for (const { cfName, filePath } of sstFiles) {
    console.log(`${cfName} 컬럼 패밀리에 ingest 중...`);
    // 현재 Node.js 바인딩에서는 컬럼 패밀리 지정이 제대로 구현되지 않음
    // 기본 컬럼 패밀리에만 ingest
    await db.ingestExternalFile({
      filePaths: [filePath],
      moveFiles: false,
      snapshotConsistency: true,
      allowGlobalSeqno: true,
      allowBlockingFlush: true,
    });
  }

  const endIngest = Date.now();
  console.log(
    `모든 컬럼 패밀리 ingest 완료! 소요 시간: ${(
      (endIngest - startIngest) /
      1000
    ).toFixed(2)}초`
  );

  // 6. 데이터 검증 (정밀 검증)
  console.log("=== 정밀 데이터 검증 시작 ===");

  // 6-1. 직접 DB 데이터 순차 검증 (1,000개)
  console.log("\n6-1. 직접 DB 데이터 순차 검증 (1,000개):");
  let directErrors = 0;
  for (let i = 1; i <= DIRECT_COUNT; i++) {
    const key = `direct:${i.toString().padStart(10, "0")}`;
    const expectedValue = `direct_value${i.toString().padStart(10, "0")}`;
    const actualValue = await db.get(key);

    if (actualValue !== expectedValue) {
      directErrors++;
      if (directErrors <= 5) {
        console.log(
          `  ❌ 오류 ${directErrors}: ${key} = ${actualValue} (예상: ${expectedValue})`
        );
      }
    }

    // 진행상황 표시
    if (i % 100 === 0) {
      console.log(`  진행: ${i}/${DIRECT_COUNT} 검증 완료`);
    }
  }
  console.log(
    `  직접 DB 데이터 검증 완료: ${
      DIRECT_COUNT - directErrors
    }/${DIRECT_COUNT} 정확 (오류: ${directErrors}개)`
  );

  // 6-2. SST ingest 데이터 순차 검증 (각 컬럼 패밀리별)
  console.log("\n6-2. SST ingest 데이터 순차 검증 (각 컬럼 패밀리별):");

  let totalSstErrors = 0;
  let totalSstSampleSize = 0;

  for (const cfName of columnFamilies) {
    console.log(`\n${cfName} 컬럼 패밀리 검증:`);
    let sstErrors = 0;
    const cfSstCount = Math.floor(sstCount / columnFamilies.length);
    const sstSampleSize = Math.min(5000, cfSstCount); // 각 컬럼 패밀리별 최대 5천개 샘플링
    const sstStep = Math.floor(cfSstCount / sstSampleSize);

    for (let i = 0; i < sstSampleSize; i++) {
      const index =
        i * sstStep +
        DIRECT_COUNT +
        1 +
        columnFamilies.indexOf(cfName) * cfSstCount;
      const key = `${cfName}:${index.toString().padStart(10, "0")}`;
      const expectedValue = `${cfName}_value${index
        .toString()
        .padStart(10, "0")}`;

      // 현재 바인딩에서는 컬럼 패밀리별 접근이 제대로 구현되지 않음
      // 기본 컬럼 패밀리에서만 접근 가능
      const actualValue = await db.get(key);

      if (actualValue !== expectedValue) {
        sstErrors++;
        if (sstErrors <= 5) {
          console.log(
            `  ❌ 오류 ${sstErrors}: ${key} = ${actualValue} (예상: ${expectedValue})`
          );
        }
      }

      // 진행상황 표시
      if (i % 1000 === 0) {
        console.log(`  진행: ${i}/${sstSampleSize} 검증 완료`);
      }
    }

    totalSstErrors += sstErrors;
    totalSstSampleSize += sstSampleSize;

    console.log(
      `  ${cfName} 데이터 검증 완료: ${
        sstSampleSize - sstErrors
      }/${sstSampleSize} 정확 (오류: ${sstErrors}개)`
    );
  }

  // 6-3. 경계점 검증 (직접 DB 마지막 ↔ SST 첫 번째)
  console.log("\n6-3. 경계점 검증:");
  const lastDirectKey = `direct:${DIRECT_COUNT.toString().padStart(10, "0")}`;
  const firstUsersKey = `users:${(DIRECT_COUNT + 1)
    .toString()
    .padStart(10, "0")}`;
  const firstProductsKey = `products:${(
    DIRECT_COUNT +
    1 +
    Math.floor(sstCount / columnFamilies.length)
  )
    .toString()
    .padStart(10, "0")}`;

  const lastDirectValue = await db.get(lastDirectKey);
  const firstUsersValue = await db.get(firstUsersKey);
  const firstProductsValue = await db.get(firstProductsKey);

  console.log(`  직접 DB 마지막: ${lastDirectKey} = ${lastDirectValue}`);
  console.log(`  Users 첫 번째: ${firstUsersKey} = ${firstUsersValue}`);
  console.log(
    `  Products 첫 번째: ${firstProductsKey} = ${firstProductsValue}`
  );

  const lastDirectExpected = `direct_value${DIRECT_COUNT.toString().padStart(
    10,
    "0"
  )}`;
  const firstUsersExpected = `users_value${(DIRECT_COUNT + 1)
    .toString()
    .padStart(10, "0")}`;
  const firstProductsExpected = `products_value${(
    DIRECT_COUNT +
    1 +
    Math.floor(sstCount / columnFamilies.length)
  )
    .toString()
    .padStart(10, "0")}`;

  console.log(
    `  직접 DB 마지막 정확: ${
      lastDirectValue === lastDirectExpected ? "✅" : "❌"
    }`
  );
  console.log(
    `  Users 첫 번째 정확: ${
      firstUsersValue === firstUsersExpected ? "✅" : "❌"
    }`
  );
  console.log(
    `  Products 첫 번째 정확: ${
      firstProductsValue === firstProductsExpected ? "✅" : "❌"
    }`
  );

  // 6-4. 전체 데이터 순서 검증 (이터레이터)
  console.log("\n6-4. 전체 데이터 순서 검증:");
  let iteratorCount = 0;
  let prevKey = null;
  let orderErrors = 0;
  const totalIterator = db.iterator();

  for await (const [key, value] of totalIterator) {
    iteratorCount++;

    // 키 순서 검증
    if (prevKey && key <= prevKey) {
      orderErrors++;
      if (orderErrors <= 3) {
        console.log(`  ❌ 순서 오류 ${orderErrors}: ${prevKey} > ${key}`);
      }
    }
    prevKey = key;

    // 진행상황 표시
    if (iteratorCount % 1000000 === 0) {
      console.log(`  진행: ${iteratorCount}개 처리됨`);
    }
  }

  console.log(`  전체 데이터 개수: ${iteratorCount}개 (예상: ${NUM}개)`);
  console.log(`  순서 오류: ${orderErrors}개`);
  console.log(`  데이터 개수 정확: ${iteratorCount === NUM ? "✅" : "❌"}`);

  // 6-5. 랜덤 샘플 확대 검증 (100개):
  console.log("\n6-5. 랜덤 샘플 확대 검증 (100개):");
  let randomErrors = 0;

  // 직접 DB 랜덤 샘플 (50개)
  for (let i = 0; i < 50; i++) {
    const randomIndex = Math.floor(Math.random() * DIRECT_COUNT) + 1;
    const key = `direct:${randomIndex.toString().padStart(10, "0")}`;
    const expectedValue = `direct_value${randomIndex
      .toString()
      .padStart(10, "0")}`;
    const actualValue = await db.get(key);

    if (actualValue !== expectedValue) {
      randomErrors++;
      console.log(
        `  ❌ 직접 DB 랜덤 오류: ${key} = ${actualValue} (예상: ${expectedValue})`
      );
    }
  }

  // SST 랜덤 샘플 (각 컬럼 패밀리별 25개씩)
  for (const cfName of columnFamilies) {
    const cfSstCount = Math.floor(sstCount / columnFamilies.length);
    for (let i = 0; i < 25; i++) {
      const randomIndex =
        Math.floor(Math.random() * cfSstCount) +
        DIRECT_COUNT +
        1 +
        columnFamilies.indexOf(cfName) * cfSstCount;
      const key = `${cfName}:${randomIndex.toString().padStart(10, "0")}`;
      const expectedValue = `${cfName}_value${randomIndex
        .toString()
        .padStart(10, "0")}`;
      const actualValue = await db.get(key);

      if (actualValue !== expectedValue) {
        randomErrors++;
        console.log(
          `  ❌ SST 랜덤 오류: ${key} = ${actualValue} (예상: ${expectedValue})`
        );
      }
    }
  }

  console.log(
    `  랜덤 샘플 검증 완료: ${
      100 - randomErrors
    }/100 정확 (오류: ${randomErrors}개)`
  );

  // 6-6. 범위 검색 정확성 검증
  console.log("\n6-6. 범위 검색 정확성 검증:");

  // 직접 DB 범위
  const directRangeIterator = db.iterator({
    gte: "direct:0000000001",
    lte: "direct:0000000010",
  });
  let directRangeCount = 0;
  for await (const [key, value] of directRangeIterator) {
    directRangeCount++;
  }
  console.log(
    `  직접 DB 범위 (1-10): ${directRangeCount}개 (예상: 10개) ${
      directRangeCount === 10 ? "✅" : "❌"
    }`
  );

  // SST 범위 (users 컬럼 패밀리)
  const sstRangeIterator = db.iterator({
    gte: `users:${(DIRECT_COUNT + 1).toString().padStart(10, "0")}`,
    lte: `users:${(DIRECT_COUNT + 10).toString().padStart(10, "0")}`,
  });
  let sstRangeCount = 0;
  for await (const [key, value] of sstRangeIterator) {
    sstRangeCount++;
  }
  console.log(
    `  SST 범위 (${DIRECT_COUNT + 1}-${
      DIRECT_COUNT + 10
    }): ${sstRangeCount}개 (예상: 10개) ${sstRangeCount === 10 ? "✅" : "❌"}`
  );

  // 6-7. 최종 요약
  console.log("\n=== 정밀 검증 최종 요약 ===");
  console.log(
    `총 데이터 개수: ${iteratorCount}/${NUM} ${
      iteratorCount === NUM ? "✅" : "❌"
    }`
  );
  console.log(
    `직접 DB 오류: ${directErrors}/${DIRECT_COUNT} ${
      directErrors === 0 ? "✅" : "❌"
    }`
  );
  console.log(
    `SST 샘플 오류: ${totalSstErrors}/${totalSstSampleSize} ${
      totalSstErrors === 0 ? "✅" : "❌"
    }`
  );
  console.log(`순서 오류: ${orderErrors}개 ${orderErrors === 0 ? "✅" : "❌"}`);
  console.log(
    `랜덤 샘플 오류: ${randomErrors}/100 ${randomErrors === 0 ? "✅" : "❌"}`
  );
  console.log(
    `범위 검색 정확: ${
      directRangeCount === 10 && sstRangeCount === 10 ? "✅" : "❌"
    }`
  );

  const totalErrors =
    directErrors + totalSstErrors + orderErrors + randomErrors;
  console.log(`\n총 오류 개수: ${totalErrors}개`);
  console.log(
    `데이터 정확도: ${(((NUM - totalErrors) / NUM) * 100).toFixed(6)}%`
  );

  await db.close();
  console.log("\n=== 정밀 검증 완료 ===");
}

benchmarkSstIngest().catch(console.error);

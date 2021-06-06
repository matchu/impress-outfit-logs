const S3 = require("aws-sdk/clients/s3");
const PromisePool = require("es6-promise-pool");
const promiseRetry = require("promise-retry");

const { backupImage, loadOutfitData } = require("./backup-image");
const LRUCache = require("lru-cache");

const NUM_WORKERS = 20;

async function main() {
  const s3 = new S3({
    params: { Bucket: "openneo-uploads" },
    region: "us-east-1",
  });

  let lastKey = process.argv[2] || null;
  let numKeys = 0;
  let numImageKeys = 0;
  let numImageKeyNoOps = 0;
  let numImageBackupKeys = 0;
  const backupFailures = [];
  for (let pageNum = 1; true; pageNum++) {
    let keys;
    try {
      keys = await promiseRetry(
        (retry, number) =>
          getImageKeys(s3, lastKey).catch((err) => {
            console.warn(
              `Error loading keys from S3, retrying (StartAfter=${lastKey}, retry=${number})`,
              err
            );
            retry(err);
          }),
        {
          retries: 10,
        }
      );
    } catch (err) {
      console.error(
        `Error loading keys from S3, giving up (StartAfter=${lastKey}):`,
        err
      );
      return 1;
    }

    if (keys.length === 0) {
      break;
    }

    console.info(
      `Page ${pageNum}: ${keys[0]} to ${keys[keys.length - 1]} ` +
        `(${keys.length} keys)`
    );

    const imageKeys = keys.filter((key) => key.endsWith(".png"));
    const imageBackupKeys = keys.filter((key) => key.endsWith(".png.bkup"));

    numKeys += keys.length;
    numImageKeys += imageKeys.length;
    numImageBackupKeys += imageBackupKeys.length;

    let imageKeyIndex = 0;
    const backupImagePromiseProducer = () => {
      if (imageKeyIndex < imageKeys.length) {
        const key = imageKeys[imageKeyIndex];
        imageKeyIndex++;
        return backupImageWithRetries(s3, key)
          .then((didMakeChanges) => {
            if (!didMakeChanges) {
              numImageKeyNoOps += 1;
            }
          })
          .catch((error) => {
            console.error(`Error backing up ${key}, giving up:`, error);
            backupFailures.push({ key, error });
          });
      } else {
        return null;
      }
    };

    const pool = new PromisePool(backupImagePromiseProducer, NUM_WORKERS);
    await pool.start();

    lastKey = keys[keys.length - 1];
  }

  const numOtherKeys = numKeys - numImageKeys - numImageBackupKeys;

  const numSuccesses = numImageKeys - numImageKeyNoOps - backupFailures.length;
  const numFailures = backupFailures.length;

  console.info(`Done!`);
  console.info(`Failed keys (count: ${backupFailures.length}):`);
  for (const { key, error } of backupFailures) {
    console.info(`- ${key} (${error.message})`);
  }
  console.info(`Summary:`);
  console.info(`- ${numImageKeys} image keys (backed up!)`);
  console.info(
    `  - ${numSuccesses} successes, ${numImageKeyNoOps} no-ops, ${numFailures} failures`
  );
  console.info(`- ${numImageBackupKeys} backup image keys (skipped!)`);
  console.info(`- ${numOtherKeys} other keys (skipped!)`);
  console.info(`- ${numKeys} total`);
}

async function getImageKeys(s3, startAfter) {
  const res = await s3
    .listObjectsV2({
      MaxKeys: 1000,
      StartAfter: startAfter,
      Prefix: "outfits/",
    })
    .promise();
  return res.Contents.map((obj) => obj.Key);
}

// Even more aggressive than caching the outfit data, we cache the outfit
// data fetching *promise*. This means that, if multiple requests come in
// for the same outfit in succession, *while the first outfit data request
// is still loading*, they'll use a single request instead of making their
// own.
//
// I only expect NUM_WORKERS entries to be possibly relevant, since we
// should process outfits in batches because the keys are adjacent.
// But I double it just to lean on the side of over-caching!
const OUTFIT_DATA_PROMISES_CACHE = new LRUCache(NUM_WORKERS * 2);

async function loadOutfitDataWithCaching(outfitId) {
  const cachedOutfitDataPromise = OUTFIT_DATA_PROMISES_CACHE.get(outfitId);
  if (cachedOutfitDataPromise) {
    return await cachedOutfitDataPromise;
  }

  const outfitDataPromise = loadOutfitData(outfitId);
  OUTFIT_DATA_PROMISES_CACHE.set(outfitId, outfitDataPromise);
  return await outfitDataPromise;
}

async function backupImageWithRetries(s3, key) {
  return await promiseRetry(
    (retry, number) => {
      // Read the outfit ID segments from the key, join them, and strip leading 0s.
      const outfitId = String(Number(key.split("/").slice(1, 4).join("")));
      return backupImage(s3, key, () =>
        loadOutfitDataWithCaching(outfitId)
      ).catch((err) => {
        console.error(`Error backing up ${key} (retry=${number}):`, err);
        retry(err);
        return false;
      });
    },
    { retries: 5 }
  );
}

main()
  .then((responseCode = 0) => process.exit(responseCode))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

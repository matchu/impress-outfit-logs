const S3 = require("aws-sdk/clients/s3");
const PromisePool = require("es6-promise-pool");
const promiseRetry = require("promise-retry");
const { timeout } = require("promise-timeout");

const NUM_WORKERS = 30;

async function main() {
  const s3 = new S3({
    params: { Bucket: "openneo-uploads" },
    region: "us-east-1",
  });

  let lastKey = process.argv[2] || null;
  let numKeys = 0;
  let numImageKeys = 0;
  const deleteFailures = [];
  for (let pageNum = 1; true; pageNum++) {
    let keys;
    try {
      keys = await promiseRetry(
        (retry, number) =>
          timeout(getImageKeys(s3, lastKey), 5000).catch((err) => {
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

    const imageKeys = keys.filter((key) => key.endsWith(".png") || key.endsWith(".png.bkup"));

    numKeys += keys.length;
    numImageKeys += imageKeys.length;

    let imageKeyIndex = 0;
    const deleteTagsPromiseProducer = () => {
      if (imageKeyIndex < imageKeys.length) {
        const key = imageKeys[imageKeyIndex];
        imageKeyIndex++;
        return deleteImageTags(s3, key)
          .catch((error) => {
            console.error(`Error backing up ${key}, giving up:`, error);
            deleteFailures.push({ key, error });
          });
      } else {
        return null;
      }
    };

    const pool = new PromisePool(deleteTagsPromiseProducer, NUM_WORKERS);
    await pool.start();

    lastKey = keys[keys.length - 1];
  }

  const numOtherKeys = numKeys - numImageKeys;

  const numSuccesses = numImageKeys - deleteFailures.length;
  const numFailures = deleteFailures.length;

  console.info(`Done!`);
  console.info(`Failed keys (count: ${deleteFailures.length}):`);
  for (const { key, error } of deleteFailures) {
    console.info(`- ${key} (${error.message})`);
  }
  console.info(`Summary:`);
  console.info(`- ${numImageKeys} image keys (backed up!)`);
  console.info(
    `  - ${numSuccesses} successes, ${numFailures} failures`
  );
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

async function deleteImageTags(s3, key) {
  return await promiseRetry(
    (retry, number) => {
      return timeout(
        s3.deleteObjectTagging({Key: key}).promise(),
        10000
      ).then(() => {
        console.info(`[${key}] Successfully deleted tags`);
      }).catch((err) => {
        console.error(`Error deleting tags from ${key} (retry=${number}):`, err);
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

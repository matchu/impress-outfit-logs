const S3 = require("aws-sdk/clients/s3");

async function main() {
  const key = process.argv[2];
  if (!key) {
    throw new Error(`backup-image must receive a key parameter on the CLI`);
  }

  const s3 = new S3({
    params: { Bucket: "openneo-uploads" },
    region: "us-east-1",
  });

  let image;
  try {
    image = await s3.getObject({ Key: key }).promise();
  } catch (err) {
    if (err.code === "NoSuchKey") {
      console.error(`Key ${key} not found`);
      return 1;
    }
    throw err;
  }

  await Promise.all([saveBackupIfNotExists(s3, key, image)]);
}

async function saveBackupIfNotExists(s3, key, image) {
  const backupKey = key + ".bkup";
  const backupTagging = await loadImageTagging(s3, backupKey);

  let shouldSaveBackup;
  if (!backupTagging) {
    shouldSaveBackup = true;
  } else if (backupTagging["DTI-Outfit-Image-Kind"] !== "backup") {
    console.warn(
      `[${key}] WARN: Skipping backup, unexpected DTI-Outfit-Image-Kind: ${backupTagging["DTI-Outfit-Image-Kind"]}`
    );
    shouldSaveBackup = false;
  } else {
    console.info(`[${key}] Backup already exists, skipping`);
    shouldSaveBackup = false;
  }

  if (shouldSaveBackup) {
    await s3
      .putObject({
        Key: backupKey,
        Body: image.Body,
        Tagging: "DTI-Outfit-Image-Kind=backup",
        StorageClass: "GLACIER",
      })
      .promise();
    console.info(`[${key}] Saved backup to ${backupKey}`);
  }
}

async function loadImageTagging(s3, key) {
  try {
    const tagResponse = await s3.getObjectTagging({ Key: key }).promise();
    const tagging = {};
    for (const tag of tagResponse.TagSet) {
      tagging[tag.Key] = tag.Value;
    }
    return tagging;
  } catch (err) {
    if (err.code === "NoSuchKey") {
      return null;
    } else {
      throw err;
    }
  }
}

main()
  .then((responseCode = 0) => process.exit(responseCode))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

const stream = require("stream");

const S3 = require("aws-sdk/clients/s3");
const PngQuant = require("pngquant");

const force = process.argv.includes("--force");

async function main() {
  const outfitId = process.argv[2];
  if (!outfitId) {
    throw new Error(
      `backup-image must receive an outfitId parameter on the CLI`
    );
  }

  const s3 = new S3({
    params: { Bucket: "openneo-uploads" },
    region: "us-east-1",
  });

  const pid = outfitId.padStart(9, "0");
  const keyPrefix =
    `outfits/${pid.substr(0, 3)}` +
    `/${pid.substr(3, 3)}` +
    `/${pid.substr(6, 3)}`;

  await Promise.all([
    backupImage(s3, keyPrefix + "/preview.png"),
    backupImage(s3, keyPrefix + "/medium_preview.png"),
    backupImage(s3, keyPrefix + "/small_preview.png"),
  ]);
}

async function backupImage(s3, key) {
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

  await saveBackupIfNotAlreadyDone(s3, key, image);
  await compressOriginalIfNotAlreadyDone(s3, key, image);
}

async function saveBackupIfNotAlreadyDone(s3, key, image) {
  const backupKey = key + ".bkup";

  if (!force) {
    const backupTagging = await loadImageTagging(s3, backupKey);

    if (backupTagging) {
      if (backupTagging["DTI-Outfit-Image-Kind"] !== "backup") {
        console.warn(
          `[WARN, ${key}] Skipping backup, unexpected DTI-Outfit-Image-Kind: ${backupTagging["DTI-Outfit-Image-Kind"]}`
        );
      } else {
        console.info(`[BKUP, ${key}] Backup already exists, skipping`);
      }
      return;
    }
  }

  await s3
    .putObject({
      Key: backupKey,
      Body: image.Body,
      ContentType: "image/png",
      Tagging: "DTI-Outfit-Image-Kind=backup",
      StorageClass: "GLACIER",
    })
    .promise();
  console.info(`[BKUP, ${key}] Saved backup to ${backupKey}`);
}

async function compressOriginalIfNotAlreadyDone(s3, key, image) {
  if (!force) {
    // Check the tags of the original image. We'll only proceed if there is no
    // DTI-Outfit-Image-Kind tag. (If it's already marked as compressed, then
    // we've done this before, and we can skip it! If it's marked with an
    // unfamiliar tag, show a warning and skip out of caution.)
    const tagging = await loadImageTagging(s3, key);
    if (!tagging) {
      throw new Error(
        `[ERRR, ${key}] Image existed earlier, but tagging not found now`
      );
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compressed") {
      console.info(`[CMPR, ${key}] Original is already compressed, skipping`);
      return;
    } else if (
      tagging["DTI-Outfit-Image-Kind"] &&
      tagging["DTI-Outfit-Image-Kind"] !== "compressed"
    ) {
      console.warn(
        `[WARN, ${key}] Skipping compression, unexpected DTI-Outfit-Image-Kind: ${tagging["DTI-Outfit-Image-Kind"]}`
      );
      return;
    }
  }

  const quanter = new PngQuant([256, "--quality", "60-80"]);
  const imageStream = stream.Readable.from(image.Body);

  // Stream the original image data into the quanter, and read the output
  // chunks from the stream one at a time, into a new Buffer.
  let compressedImageData = Buffer.alloc(0);
  await new Promise((resolve, reject) => {
    imageStream.pipe(quanter);

    quanter.on("error", (err) => reject(err));
    quanter.on("data", (chunk) => {
      compressedImageData = Buffer.concat([compressedImageData, chunk]);
    });
    quanter.on("end", () => {
      resolve();
    });
  });

  const originalSize = humanFileSize(image.Body.length);
  const compressedSize = humanFileSize(compressedImageData.length);
  const compressedPercent = Math.round(
    (compressedImageData.length / image.Body.length) * 100
  );
  console.info(
    `[CMPR, ${key}] Compressed image: ${originalSize} -> ${compressedSize} (${compressedPercent}% of original)`
  );

  await s3
    .putObject({
      Key: key,
      Body: compressedImageData,
      ContentType: "image/png",
      ACL: "public-read",
      Tagging: "DTI-Outfit-Image-Kind=compressed",
      // TODO: Consider `StorageClass: "STANDARD_IA"… The gist is that it
      //       would cut storage costs in half, but double request costs,
      //       and add 10% to data transfer costs. My hunch is actually
      //       that, once I clear out the storage to only include
      //       relatively frequently-accessed things, and compress them,
      //       this won't be a great trade anymore? We can migrate them
      //       later if that turns out to be the case.
      // TODO: But we probably want infrequent access for the "Not Found"
      //       placeholder images we're gonna add, because we have no
      //       real reason to believe they'll be accessed hardly *ever*!
    })
    .promise();
  console.info(`[SAVE, ${key}] Saved compressed image to ${key}`);
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

// https://stackoverflow.com/a/14919494/107415
function humanFileSize(bytes, si = false, dp = 1) {
  const thresh = si ? 1000 : 1024;

  if (Math.abs(bytes) < thresh) {
    return bytes + " B";
  }

  const units = si
    ? ["kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    : ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
  let u = -1;
  const r = 10 ** dp;

  do {
    bytes /= thresh;
    ++u;
  } while (
    Math.round(Math.abs(bytes) * r) / r >= thresh &&
    u < units.length - 1
  );

  return bytes.toFixed(dp) + " " + units[u];
}

main()
  .then((responseCode = 0) => process.exit(responseCode))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

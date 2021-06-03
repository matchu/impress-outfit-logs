const stream = require("stream");

const fetch = require("node-fetch");
const S3 = require("aws-sdk/clients/s3");
const PngQuant = require("pngquant");
const gql = require("graphql-tag");
const { print: graphqlPrint } = require("graphql/language/printer");

const {
  getVisibleLayers,
  petAppearanceFragmentForGetVisibleLayers,
  itemAppearanceFragmentForGetVisibleLayers,
} = require("./lib/getVisibleLayers");
const { renderOutfitImage } = require("./lib/outfit-images");

// Adapted from https://github.com/matchu/impress-2020/blob/f932498066d6a35a778db3cdf600de62be438c6e/api/outfitImage.js#L172
// Only change is to request all the sizes!
const GRAPHQL_QUERY = gql`
  query ApiOutfitImage($outfitId: ID!) {
    outfit(id: $outfitId) {
      petAppearance {
        layers {
          imageUrl600: imageUrl(size: SIZE_600)
          imageUrl300: imageUrl(size: SIZE_300)
          imageUrl150: imageUrl(size: SIZE_150)
        }
        ...PetAppearanceForGetVisibleLayers
      }
      itemAppearances {
        layers {
          imageUrl600: imageUrl(size: SIZE_600)
          imageUrl300: imageUrl(size: SIZE_300)
          imageUrl150: imageUrl(size: SIZE_150)
        }
        ...ItemAppearanceForGetVisibleLayers
      }
    }
  }
  ${petAppearanceFragmentForGetVisibleLayers}
  ${itemAppearanceFragmentForGetVisibleLayers}
`;
const GRAPHQL_QUERY_STRING = graphqlPrint(GRAPHQL_QUERY);

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

  let outfitDataPromise;
  const getOutfitData = () => {
    if (!outfitDataPromise) {
      outfitDataPromise = fetch(
        "https://impress-2020.openneo.net/api/graphql",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            query: GRAPHQL_QUERY_STRING,
            variables: { outfitId },
          }),
        }
      ).then((res) => res.json());
    }
    return outfitDataPromise;
  };

  await Promise.all([
    backupImage(s3, outfitId, 600, getOutfitData),
    backupImage(s3, outfitId, 300, getOutfitData),
    backupImage(s3, outfitId, 150, getOutfitData),
  ]);
}

const SIZE_TO_FILENAME_MAP = {
  600: "preview.png",
  300: "medium_preview.png",
  150: "small_preview.png",
};

async function backupImage(s3, outfitId, size, getOutfitData) {
  const pid = outfitId.padStart(9, "0");
  const key =
    `outfits/${pid.substr(0, 3)}` +
    `/${pid.substr(3, 3)}` +
    `/${pid.substr(6, 3)}` +
    `/${SIZE_TO_FILENAME_MAP[size]}`;

  const tagging = await loadImageTagging(s3, key);
  if (!tagging) {
    console.error(`[ERRR, ${key}] Image not found`);
    return;
  }

  await saveBackupIfNotAlreadyDone(s3, key);
  await compressOriginalIfNotAlreadyDone(s3, key, size, tagging, getOutfitData);
}

async function saveBackupIfNotAlreadyDone(s3, key) {
  const backupKey = key + ".bkup";

  if (!force) {
    const backupTagging = await loadImageTagging(s3, backupKey);

    if (backupTagging) {
      if (backupTagging["DTI-Outfit-Image-Kind"] !== "backup") {
        console.warn(
          `[WARN, ${key}] Skipping backup, unexpected DTI-Outfit-Image-Kind: ${backupTagging["DTI-Outfit-Image-Kind"]}`
        );
        return;
      } else {
        console.info(`[BKUP, ${key}] Backup already exists, skipping`);
        return;
      }
    }
  }

  await s3
    .copyObject({
      Key: backupKey,
      CopySource: `/openneo-uploads/${key}`,
      ContentType: "image/png",
      Tagging: "DTI-Outfit-Image-Kind=backup",
      TaggingDirective: "REPLACE",
      StorageClass: "GLACIER",
    })
    .promise();
  console.info(`[BKUP, ${key}] Saved backup to ${backupKey}`);
}

async function compressOriginalIfNotAlreadyDone(
  s3,
  key,
  size,
  tagging,
  getOutfitData
) {
  if (!force) {
    // Check the tags of the original image. We'll only proceed if there is no
    // DTI-Outfit-Image-Kind tag. (If it's already marked as compressed, then
    // we've done this before, and we can skip it! If it's marked with an
    // unfamiliar tag, show a warning and skip out of caution.)
    if (!tagging) {
      throw new Error(`[ERRR, ${key}] Image not found`);
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compressed") {
      console.info(`[CMPR, ${key}] Original is already compressed, skipping`);
      return;
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compression-failed") {
      console.info(
        `[CMPR, ${key}] Original previously failed to compress, skipping`
      );
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

  const { data, errors } = await getOutfitData();
  if (errors && errors.length > 0) {
    console.error(`[ERRR, ${key}] GraphQL outfit query failed:`, errors);
    return;
  }
  if (!data.outfit) {
    console.error(
      `[ERRR, ${key}] GraphQL outfit query failed: ${outfitId} not found`
    );
    return;
  }

  const { petAppearance, itemAppearances } = data.outfit;
  const visibleLayers = getVisibleLayers(petAppearance, itemAppearances)
    .sort((a, b) => a.depth - b.depth)
    .map((layer) => layer["imageUrl" + size]);

  const { image, status } = await renderOutfitImage(visibleLayers, size);
  if (status !== "success") {
    console.error(
      `[ERRR, ${key}] Could not render outfit image. Status: ${status}`
    );
    return;
  }

  // We instruct the algorithm to target 80% quality, but we'll accept down
  // to 40% quality. Sometimes it won't be possible to compress the image
  // without decreasing the quality further (in which case, the algorithm
  // might yield an image *larger* than the original). In that situation,
  // we'd rather just keep the larger version, than go *so* low in quality!
  const quanter = new PngQuant([256, "--quality", "40-80"]);
  const imageStream = stream.Readable.from(image);

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

  const originalSize = image.length;
  const compressedSize = compressedImageData.length;
  const compressedPercent = Math.round((compressedSize / originalSize) * 100);

  // If we couldn't compress the image without compromising quality (so the
  // compression algorithm yielded a larger image), don't write it, and instead
  // set `DTI-Outfit-Image-Kind=compression-failed` on the image. This will help
  // us know that it's done, and skip it if we try again later. We also want to
  // move it to STANDARD_IA in this case, regardless of compression!
  if (compressedSize > originalSize) {
    console.warn(
      `[WARN, ${key}] Skipping compression, was ` +
        `${humanFileSize(originalSize)} -> ${humanFileSize(compressedSize)} ` +
        `(${compressedPercent}% of original)`
    );

    // To update the tags and the storage class, copy the object over itself.
    await s3
      .copyObject({
        Key: key,
        CopySource: `/openneo-uploads/${key}`,
        Tagging: "DTI-Outfit-Image-Kind=compression-failed",
        TaggingDirective: "REPLACE",
        // We ran the numbers, and our request counts aren't even close to high enough
        // for STANDARD to be better for us!
        StorageClass: "STANDARD_IA",
      })
      .promise();

    return;
  }

  console.info(
    `[CMPR, ${key}] Compressed image: ` +
      `${humanFileSize(originalSize)} -> ${humanFileSize(compressedSize)} ` +
      `(${compressedPercent}% of original)`
  );

  await s3
    .putObject({
      Key: key,
      Body: compressedImageData,
      ContentType: "image/png",
      ACL: "public-read",
      Tagging: "DTI-Outfit-Image-Kind=compressed",
      // We ran the numbers, and our request counts aren't even close to high enough
      // for STANDARD to be better for us!
      StorageClass: "STANDARD_IA",
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

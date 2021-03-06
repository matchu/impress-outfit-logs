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

// HACK: Use Node's testing APIs to be able to log custom trace
//       events. I… genuinely didn't find a better way to do
//       local nodejs custom tracing??
//
// To enable tracing, run this with:
//     node --expose-internals --trace-event-categories app
//
// Without that, the require will throw an error, so the trace
// functions will be no-ops!
let trace;
let withTrace;
try {
  const { internalBinding } = require("internal/test/binding");
  const rawTrace = internalBinding("trace_events").trace;

  // `trace` lets you wrap an async block with start/end trace events!
  // Like: `await trace("fetchThatOneWebpage", {url}, () => fetch(url))`
  let nextTraceId = 0;
  trace = async (eventName, traceArgs, fn) => {
    let traceId = nextTraceId++;
    rawTrace("b".charCodeAt(0), "app", eventName, traceId, traceArgs);
    try {
      return await fn();
    } finally {
      rawTrace("e".charCodeAt(0), "app", eventName, traceId, traceArgs);
    }
  };

  // `withTrace` lets you wrap an async function with start/end trace
  // events for every time you call it! First parameter is the function,
  // second parameter is a function to transform the arguments into an
  // object of safe, serializable arguments to log with the trace.
  withTrace =
    (fn, getTraceArgsFromFnArgs, name = null) =>
    (...args) =>
      trace(name || fn.name, getTraceArgsFromFnArgs(...args), () =>
        fn(...args)
      );
} catch (e) {
  trace = (_, __, fn) => fn();
  withTrace = (fn) => fn;
}

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

  // NOTE: We preload outfit data, even though we might not end up using it.
  //       This helps us parallelize things better, to not bottleneck on it!
  const outfitDataPromise = loadOutfitData(outfitId);
  const getOutfitData = async () => await outfitDataPromise;

  const pid = outfitId.padStart(9, "0");
  const baseKey =
    `outfits/${pid.substr(0, 3)}` +
    `/${pid.substr(3, 3)}` +
    `/${pid.substr(6, 3)}`;

  const handleError = (key, err) => {
    console.error(`[ERRR, ${key}]`, err);
  };

  await Promise.all([
    backupImage(s3, baseKey + "/preview.png", getOutfitData).catch((err) =>
      handleError(baseKey + "/preview.png", err)
    ),
    backupImage(s3, baseKey + "/medium_preview.png", getOutfitData).catch(
      (err) => handleError(baseKey + "/medium_preview.png", err)
    ),
    backupImage(s3, baseKey + "/small_preview.png", getOutfitData).catch(
      (err) => handleError(baseKey + "/small_preview.png", err)
    ),
  ]);
}

const FILENAME_TO_SIZE_MAP = {
  "preview.png": 600,
  "medium_preview.png": 300,
  "small_preview.png": 150,
};

async function backupImage(s3, key, getOutfitData) {
  const backupKey = key + ".bkup";

  // NOTE: We preload the compressed image, even if we might not end up
  //       using it (in the case of an error during backup). This helps us
  //       parallelize things better, to not bottleneck on it!
  const originalImagePromise = buildOutfitImage(key, getOutfitData);
  const compressedImagePromise = originalImagePromise.then(compressImage);
  const getNewImages = async () => ({
    originalImage: await originalImagePromise,
    compressedImage: await compressedImagePromise,
  });

  // Preload the backup image tagging, too!
  const backupTaggingPromise = loadImageTagging(s3, backupKey);

  const tagging = await loadImageTagging(s3, key);
  if (!tagging) {
    throw new Error(`Image not found`);
  }

  // First, back up the original image, before touching anything else.
  const backupTagging = await backupTaggingPromise;
  const didSaveBackup = await saveBackupIfNotAlreadyDone(
    s3,
    key,
    backupKey,
    backupTagging
  );

  // Then, replace it with the new images.
  const didReplaceOriginal = await replaceOriginalIfNotAlreadyDone(
    s3,
    key,
    tagging,
    getNewImages
  );

  // Return whether we made some kind of change, either in the backup
  // or the replacement step.
  return didSaveBackup || didReplaceOriginal;
}
backupImage = withTrace(backupImage, (_, key) => ({ key }), "1. backupImage");

async function saveBackupIfNotAlreadyDone(s3, key, backupKey, backupTagging) {
  if (!force) {
    if (backupTagging) {
      if (backupTagging["DTI-Outfit-Image-Kind"] !== "backup") {
        console.warn(
          `[WARN, ${key}] Skipping backup, unexpected DTI-Outfit-Image-Kind: ${backupTagging["DTI-Outfit-Image-Kind"]}`
        );
        return false;
      } else {
        console.info(`[BKUP, ${key}] Backup already exists, skipping`);
        return false;
      }
    }
  }

  await trace("3b. copyObject-backup", { key }, () =>
    s3
      .copyObject({
        Key: backupKey,
        CopySource: `/openneo-uploads/${key}`,
        ContentType: "image/png",
        Tagging: "DTI-Outfit-Image-Kind=backup",
        TaggingDirective: "REPLACE",
        StorageClass: "GLACIER",
      })
      .promise()
  );
  console.info(`[BKUP, ${key}] Saved backup to ${backupKey}`);
  return true;
}
saveBackupIfNotAlreadyDone = withTrace(
  saveBackupIfNotAlreadyDone,
  (_, key, backupKey) => ({ key, backupKey }),
  "2b. saveBackupIfNotAlreadyDone"
);

async function buildNewImagesIfNotAlreadyDone(key, getOutfitData) {
  if (!force) {
    // Check the tags of the original image. We'll only proceed if there is no
    // DTI-Outfit-Image-Kind tag. (If it's already marked as compressed, then
    // we've done this before, and we can skip it! If it's marked with an
    // unfamiliar tag, show a warning and skip out of caution.)
    if (!tagging) {
      throw new Error(`Image not found`);
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compressed") {
      console.info(`[CMPR, ${key}] Original is already compressed, skipping`);
      return null;
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compression-failed") {
      console.info(
        `[CMPR, ${key}] Original previously failed to compress, skipping`
      );
      return null;
    } else if (
      tagging["DTI-Outfit-Image-Kind"] &&
      tagging["DTI-Outfit-Image-Kind"] !== "compressed"
    ) {
      console.warn(
        `[WARN, ${key}] Skipping compression, unexpected DTI-Outfit-Image-Kind: ${tagging["DTI-Outfit-Image-Kind"]}`
      );
      return null;
    }
  }

  const originalImage = await buildOutfitImage(key, getOutfitData);
  const compressedImage = await compressImage(originalImage);

  return { originalImage, compressedImage };
}
buildNewImagesIfNotAlreadyDone = withTrace(
  buildNewImagesIfNotAlreadyDone,
  (key) => ({ key }),
  "2b. buildNewImagesIfNotAlreadyDone"
);

async function replaceOriginalIfNotAlreadyDone(s3, key, tagging, getNewImages) {
  if (!force) {
    // Check the tags of the original image. We'll only proceed if there is no
    // DTI-Outfit-Image-Kind tag. (If it's already marked as compressed, then
    // we've done this before, and we can skip it! If it's marked with an
    // unfamiliar tag, show a warning and skip out of caution.)
    if (!tagging) {
      throw new Error(`Image not found`);
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compressed") {
      console.info(`[CMPR, ${key}] Original is already compressed, skipping`);
      return null;
    } else if (tagging["DTI-Outfit-Image-Kind"] === "compression-failed") {
      console.info(
        `[CMPR, ${key}] Original previously failed to compress, skipping`
      );
      return null;
    } else if (
      tagging["DTI-Outfit-Image-Kind"] &&
      tagging["DTI-Outfit-Image-Kind"] !== "compressed"
    ) {
      console.warn(
        `[WARN, ${key}] Skipping compression, unexpected DTI-Outfit-Image-Kind: ${tagging["DTI-Outfit-Image-Kind"]}`
      );
      return null;
    }
  }

  const { originalImage, compressedImage } = await getNewImages();

  const originalSize = originalImage.length;
  const compressedSize = compressedImage.length;
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
    await trace("4f. copyObject-compressionFailed", { key }, () =>
      s3
        .copyObject({
          Key: key,
          CopySource: `/openneo-uploads/${key}`,
          ACL: "public-read",
          Tagging: "DTI-Outfit-Image-Kind=compression-failed",
          TaggingDirective: "REPLACE",
          // We ran the numbers, and our request counts aren't even close to high enough
          // for STANDARD to be better for us!
          StorageClass: "STANDARD_IA",
        })
        .promise()
    );

    return true;
  }

  console.info(
    `[CMPR, ${key}] Compressed image: ` +
      `${humanFileSize(originalSize)} -> ${humanFileSize(compressedSize)} ` +
      `(${compressedPercent}% of original)`
  );

  await trace("4e. putObject-compressed", { key }, () =>
    s3
      .putObject({
        Key: key,
        Body: compressedImage,
        ContentType: "image/png",
        ACL: "public-read",
        Tagging: "DTI-Outfit-Image-Kind=compressed",
        // We ran the numbers, and our request counts aren't even close to high enough
        // for STANDARD to be better for us!
        StorageClass: "STANDARD_IA",
      })
      .promise()
  );

  console.info(`[SAVE, ${key}] Saved compressed image to ${key}`);
  return true;
}
replaceOriginalIfNotAlreadyDone = withTrace(
  replaceOriginalIfNotAlreadyDone,
  (_, key) => ({ key }),
  "2c. replaceOriginalIfNotAlreadyDone"
);

async function loadOutfitData(outfitId) {
  console.info(`[GQL] Loading outfit data for outfit ${outfitId}`);
  return await fetch("https://impress-2020.openneo.net/api/graphql", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: GRAPHQL_QUERY_STRING,
      variables: { outfitId },
    }),
  }).then((res) => res.json());
}
loadOutfitData = withTrace(
  loadOutfitData,
  (outfitId) => ({ outfitId }),
  "4a. loadOutfitData"
);

async function buildOutfitImage(key, getOutfitData) {
  const { data, errors } = await getOutfitData();
  if (errors && errors.length > 0) {
    throw new Error(`GraphQL outfit query failed:\n` + JSON.stringify(errors));
  }
  if (!data.outfit) {
    throw new Error(`GraphQL outfit query failed: ${outfitId} not found`);
  }

  const filename = key.split("/").pop();
  const size = FILENAME_TO_SIZE_MAP[filename];

  const { petAppearance, itemAppearances } = data.outfit;
  const visibleLayers = getVisibleLayers(petAppearance, itemAppearances)
    .sort((a, b) => a.depth - b.depth)
    .map((layer) => layer["imageUrl" + size]);

  const { image, status } = await trace("4c. renderOutfitImage", { key }, () =>
    renderOutfitImage(visibleLayers, size)
  );
  if (status !== "success") {
    throw new Error(`Could not render outfit image. Status: ${status}`);
  }

  return image;
}
buildOutfitImage = withTrace(
  buildOutfitImage,
  (key) => ({ key }),
  "4b. buildOutfitImage"
);

async function compressImage(image) {
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

  return compressedImageData;
}
compressImage = withTrace(compressImage, () => ({}), "4d. compressImage");

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
loadImageTagging = withTrace(
  loadImageTagging,
  (_, key) => ({ key }),
  "3a. loadImageTagging"
);

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

module.exports = { backupImage, loadOutfitData };

if (require.main === module) {
  main()
    .then((responseCode = 0) => process.exit(responseCode))
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}

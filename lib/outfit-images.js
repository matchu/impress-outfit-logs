// Adapted from https://github.com/matchu/impress-2020/blob/da8cd8eda9e301c21f0320d7c95d605efeb355a0/src/server/outfit-images.js#L1
// Only change is to import/export style!
const { createCanvas, loadImage } = require("canvas");

async function renderOutfitImage(layerRefs, size) {
  const canvas = createCanvas(size, size);
  const ctx = canvas.getContext("2d");

  const images = await Promise.all(layerRefs.map(loadImageAndSkipOnFailure));
  const loadedImages = images.filter((image) => image);
  for (const image of loadedImages) {
    ctx.drawImage(image, 0, 0, size, size);
  }

  return {
    image: canvas.toBuffer(),
    status:
      loadedImages.length === layerRefs.length ? "success" : "partial-failure",
  };
}

async function loadImageAndSkipOnFailure(url) {
  if (!url) {
    console.warn(`Error loading layer, URL was nullish: ${url}`);
    return null;
  }

  try {
    const image = await loadImage(url);
    return image;
  } catch (e) {
    console.warn(`Error loading layer, skipping: ${e.message}. (${url})`);
    return null;
  }
}

module.exports = { renderOutfitImage };

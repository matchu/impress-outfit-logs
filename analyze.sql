-- Total number of logs
SELECT count(*) FROM logs;

-- Total number of outfit images logged
SELECT count(*) FROM (SELECT outfitId, imageSize FROM logs GROUP BY outfitId, imageSize);

-- Outfit images with largest log counts
SELECT outfitId, imageSize, count(*) FROM logs GROUP BY outfitId, imageSize
  ORDER BY count(*) DESC LIMIT 10;

-- Outfit image log counts, grouped into buckets
SELECT
  (
    CASE
      WHEN logCount BETWEEN 1 and 4 THEN "A. 1-4"
      WHEN logCount BETWEEN 5 AND 20 THEN "B. 5-20"
      WHEN logCount BETWEEN 21 AND 50 THEN "C. 21-50"
      WHEN logCount BETWEEN 51 AND 100 THEN "D. 51-100"
      WHEN logCount >= 101 THEN "E. 101+"
    END
  ) AS bucket,
  count(*)
  FROM (
    SELECT outfitId, imageSize, count(*) AS logCount FROM logs GROUP BY outfitId, imageSize
  )
  GROUP BY bucket
  ORDER BY bucket;

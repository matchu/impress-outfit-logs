const fs = require("fs").promises;
const path = require("path");
const util = require("util");
const zlib = require("zlib");
const gunzip = util.promisify(zlib.gunzip);

const sqlite3 = require("sqlite3").verbose();
const trash = require("trash");
const walk = require("walkdir");

const logsPath = path.join(__dirname, "logs");

async function main() {
  await trash("db.sqlite3");

  const db = new sqlite3.Database("db.sqlite3");

  db.serialize(() => {
    db.run(
      `
        CREATE TABLE logs (
          eventId    TEXT                                          NOT NULL,
          eventTime  TEXT                                          NOT NULL,

          outfitId   INTEGER                                       NOT NULL,
          imageSize  INTEGER  CHECK(imageSize IN (150, 300, 600))  NOT NULL,

          host       TEXT,
          ipAddress  TEXT,
          userAgent  TEXT,
          awsRegion  TEXT
        )
      `,
      (err) => {
        if (err) {
          throw err;
        }

        db.run(`BEGIN TRANSACTION`);

        insertLogsFromFiles(db, () => {
          db.run(`COMMIT`);

          db.get(`SELECT count(*) FROM logs`, (err, row) => {
            if (err) {
              throw err;
            }

            console.log("Count result:", row);
          });

          db.close();
        });
      }
    );
  });
}

function insertLogsFromFiles(db, callback) {
  db.parallelize(() => {
    const promises = [];

    const insertLogStmt = db.prepare(`
      INSERT INTO logs (eventId, eventTime, outfitId, imageSize, host, ipAddress, userAgent, awsRegion)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertLog = util.promisify((...args) => insertLogStmt.run(...args));

    // For each file, start an `insertLogsFromFile` job, and add a promise to the
    // list of promises we're tracking.
    const walker = walk(logsPath, (path, stat) =>
      promises.push(
        insertLogsFromFile(insertLog, path, stat).catch((err) => {
          console.error(`Error reading file ${path}`, err);
        })
      )
    );

    // Once we've walked all the files, wait for all the promises to finish, then
    // we're done!
    walker.on("end", () =>
      Promise.all(promises).then(() => {
        insertLogStmt.finalize();
        callback();
      })
    );
  });
}

async function insertLogsFromFile(insertLog, path, stat) {
  console.log(`[${path}] Start`);
  const isLogFile = stat.isFile() && path.endsWith(".json.gz");
  if (!isLogFile) {
    return;
  }

  const gzippedBody = await fs.readFile(path, null);
  const jsonBody = await gunzip(gzippedBody);
  const logs = JSON.parse(jsonBody);

  console.log(`[${path}] DB`);

  const promises = [];
  for (const record of logs.Records) {
    if (record.eventName !== "GetObject") {
      continue;
    }

    const parsedKey = parseS3Key(record.requestParameters.key);
    if (!parsedKey) {
      continue;
    }

    promises.push(
      insertLog([
        record.eventID,
        record.eventTime,
        parsedKey.outfitId,
        parsedKey.imageSize,
        record.requestParameters.Host,
        record.sourceIPAddress,
        record.userAgent,
        record.awsRegion,
      ])
    );
  }

  await Promise.all(promises);
  console.log(`[${path}] Done`);
}

const S3_KEY_PATTERN =
  /^outfits\/([0-9]{3})\/([0-9]{3})\/([0-9]{3})\/(small_preview|medium_preview|preview)\.png$/;
const FILENAME_TO_SIZE_MAP = {
  small_preview: 150,
  medium_preview: 300,
  preview: 600,
};
function parseS3Key(key) {
  const match = key.match(S3_KEY_PATTERN);
  if (!match) {
    return null;
  }

  const outfitId = Number(match[1] + match[2] + match[3]);
  const imageSize = FILENAME_TO_SIZE_MAP[match[4]];

  return { outfitId, imageSize };
}

main().catch((e) => console.error(e));

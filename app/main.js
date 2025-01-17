import { open } from "fs/promises";
import { Blob } from "buffer";
import { getPageBuffer, getPageHeader, getCells } from "./page.js";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

export const DB_FILE_HEADER_OFFSET = 100;
export const DB_METADATA = {};

export const getNBytesOfDb = async (dbOffset = 0, nBytes, bufferSize = 200) => {
  const databaseFileHandler = await open(databaseFilePath, "r");

  const { buffer } = await databaseFileHandler.read({
    length: nBytes,
    position: dbOffset,
    buffer: Buffer.alloc(bufferSize),
    offset: 0,
  });

  databaseFileHandler.close();
  return buffer;
};

const initDBMetadata = async () => {
  const dbFileHeaderBuffer = await getNBytesOfDb(
    0,
    DB_FILE_HEADER_OFFSET,
    DB_FILE_HEADER_OFFSET
  );

  const pageSize = dbFileHeaderBuffer.readUint16BE(16);
  DB_METADATA.pageSize = pageSize;
};

const getSchemaDetails = async () => {
  const pageBuffer = await getPageBuffer(1);
  const pageHeader = await getPageHeader(pageBuffer, DB_FILE_HEADER_OFFSET);
  const cells = await getCells(pageBuffer, DB_FILE_HEADER_OFFSET);
  return { pageHeader, cells };
};

const main = async () => {
  await initDBMetadata();
  const schema = await getSchemaDetails();
  if (command === ".dbinfo") {
    console.log(`database page size: ${DB_METADATA.pageSize}`);
    console.log(`number of tables: ${schema.pageHeader.cellCount}`);
  } else if (command === ".tables") {
    const tableNames = [];
    for (let cell of schema.cells) {
      tableNames.push(cell.record.cols[2]);
    }
    console.log(tableNames.join(" "));
  } else if (command.toLowerCase().startsWith("select")) {
    const capturedGroups = /select\s+(.+)\s+from\s+(.+)/i.exec(command); // i modifier for case insensitive regex

    const cols = capturedGroups[1].replaceAll(/\s+/g, "").split(",");
    const tableName = capturedGroups[2];

    const cell = schema.cells.find((cell) => {
      return cell.record.cols[2] === tableName;
    });

    const tableRootPage = cell.record.cols[3];
    const tablePageBuffer = await getPageBuffer(tableRootPage);
    const tableCells = await getCells(tablePageBuffer);

    for (let cell of schema.cells) {
      const sql = cell.record.cols[4];
      if (sql instanceof Buffer) {
        console.log(await sql.toString());
      } else {
        console.log(sql);
      }
    }

    if (/count\(\*\)/i.test(cols[0])) {
      console.log(tableCells.length);
    } else {
      // pass
    }
  } else {
    throw `Unknown command ${command}`;
  }
};

main();

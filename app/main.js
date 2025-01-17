import { open } from "fs/promises";
import { Blob } from "buffer";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

const getNBytesOfDb = async (
  databaseFilePath,
  dbOffset = 0,
  nBytes,
  bufferSize = 200
) => {
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

const readVarInt = async (databaseFilePath, startOffset) => {
  let val = 0;
  let toContinue = true;
  let bytesRead = 0;

  while (toContinue) {
    let buffer = await getNBytesOfDb(
      databaseFilePath,
      startOffset + bytesRead,
      1,
      1
    );
    const byte = buffer.readUInt8(0);
    bytesRead++;

    toContinue = byte >= 128;
    if (toContinue) {
      val += byte - 128;
    } else {
      val += byte;
    }
  }
  return [val, bytesRead];
};

const getRecordBody = async (
  databaseFilePath,
  recordBodyStartOffset,
  serialTypeCode
) => {
  let contentSize = 0;
  let content;

  if (serialTypeCode === 0) {
    return null;
  }
  if (serialTypeCode <= 6) {
    // ints
    if (serialTypeCode <= 4) {
      contentSize = serialTypeCode;
    } else if (serialTypeCode === 5) {
      contentSize = 6;
    } else {
      contentSize = 8;
    }
    const tempBuffer = await getNBytesOfDb(
      databaseFilePath,
      recordBodyStartOffset,
      contentSize,
      contentSize
    );
    if (serialTypeCode === 1) {
      content = tempBuffer.readUInt8();
    } else {
      content = tempBuffer.readUintBE();
    }
  } else if (serialTypeCode === 7) {
    // float
    contentSize = 8;
    const tempBuffer = await getNBytesOfDb(
      databaseFilePath,
      recordBodyStartOffset,
      contentSize,
      contentSize
    );
    content = tempBuffer.readFloatBE();
  } else if (serialTypeCode <= 9) {
    content = serialTypeCode - 8;
  } else if (serialTypeCode >= 12) {
    if (serialTypeCode % 2 === 0) {
      contentSize = (serialTypeCode - 12) / 2;
      const tempBuffer = await getNBytesOfDb(
        databaseFilePath,
        recordBodyStartOffset,
        contentSize,
        contentSize
      );
      content = new Blob([tempBuffer]);
    } else {
      contentSize = (serialTypeCode - 13) / 2;
      const tempBuffer = await getNBytesOfDb(
        databaseFilePath,
        recordBodyStartOffset,
        contentSize,
        contentSize
      );
      content = tempBuffer.toString();
    }
  }

  return [content, contentSize];
};

const getAllTableMetadata = async () => {
  const cellPointerArrayOffset = 108;
  let buffer = await getNBytesOfDb(databaseFilePath, 100, 8, 8);

  const cellCount = buffer.readUInt16BE(3);

  const tableNames = [];
  const tableRootPages = [];

  buffer = await getNBytesOfDb(
    databaseFilePath,
    cellPointerArrayOffset,
    2 * cellCount,
    2 * cellCount
  );

  for (let i = 0; i < cellCount; i++) {
    // For each cell
    const offset = buffer.readUInt16BE(i * 2);

    let recordSize = 0,
      rowId = 0,
      recordHeaderSize = 0,
      serialTypeCode = 0,
      bytesRead = 0,
      totalBytesRead = 0;

    let contentStartOffset = offset;

    for (let i = 0; i < 3; i++) {
      let content;
      [content, bytesRead] = await readVarInt(
        databaseFilePath,
        offset + totalBytesRead
      );
      totalBytesRead += bytesRead;
      switch (i) {
        case 0:
          recordSize = content;
          break;
        case 1:
          rowId = content;
          contentStartOffset += totalBytesRead;
          break;
        case 2:
          recordHeaderSize = content;
          contentStartOffset += recordHeaderSize;
          break;
        default:
          break;
      }
    }

    let contentBytesRead = 0;
    for (let i = 0; i < 4; i++) {
      let content;
      [serialTypeCode, bytesRead] = await readVarInt(
        databaseFilePath,
        offset + totalBytesRead
      );
      totalBytesRead += bytesRead;
      [content, bytesRead] = await getRecordBody(
        databaseFilePath,
        contentStartOffset + contentBytesRead,
        serialTypeCode
      );
      contentBytesRead += bytesRead;
      switch (i) {
        case 2: // table name of record
          tableNames.push(content);
          break;
        case 3: // rowCount of table
          tableRootPages.push(content);
          break;
        default:
          break;
      }
    }
  }

  return [tableNames, tableRootPages];
};

const getPage = async (pageNo) => {
  let buffer = await getNBytesOfDb(databaseFilePath, 16, 2, 2); // page size is 2 bytes starting at offset 16
  const pageSize = buffer.readUInt16BE(0);
  const pageOffset = (pageNo - 1) * pageSize;

  buffer = await getNBytesOfDb(
    databaseFilePath,
    pageOffset,
    pageSize,
    pageSize
  );

  return buffer;
};

if (command === ".dbinfo") {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  const buffer = await getNBytesOfDb(databaseFilePath, 0, 108, 108);

  const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  console.log(`database page size: ${pageSize}`);

  const cellCount = buffer.readUInt16BE(103); // cell count is 3 bytes starting from the end of file header
  console.log(`number of tables: ${cellCount}`);
} else if (command === ".tables") {
  const [tableNames, _tableRowCounts] = await getAllTableMetadata();
  console.log(tableNames.join(" "));
} else if (command.toLowerCase().startsWith("select count(*) from ")) {
  const tableName = command.split(" ")[3];
  const [tableNames, tableRootPages] = await getAllTableMetadata();

  const tableRootPage = tableRootPages[tableNames.indexOf(tableName)];
  const pageBuffer = await getPage(tableRootPage);
  const rowCount = pageBuffer.readUInt16BE(3);
  console.log(rowCount);
} else {
  throw `Unknown command ${command}`;
}

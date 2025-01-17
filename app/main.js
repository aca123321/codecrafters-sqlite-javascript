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
    content = tempBuffer.readUintBE();
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

if (command === ".dbinfo") {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  const buffer = await getNBytesOfDb(databaseFilePath, 0, 108, 108);
  console.error("Logs from your program will appear here!");

  // Uncomment this to pass the first stage
  const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  console.log(`database page size: ${pageSize}`);

  // const fileFormatWriteVersion = buffer.readUInt8(18);
  // console.log(`database file format write version: ${fileFormatWriteVersion}`);

  const cellCount = buffer.readUInt16BE(103); // cell count is 3 bytes starting from the end of file header
  console.log(`number of tables: ${cellCount}`);
} else if (command === ".tables") {
  const cellPointerArrayOffset = 108;
  let buffer = await getNBytesOfDb(databaseFilePath, 100, 8, 8);

  const cellCount = buffer.readUInt16BE(3);

  const cellPointers = []; // contains offsets (relative to the start of the page)
  const cellRecordSizes = [];
  const cellRowIds = [];
  const cellRecordHeaderSizes = [];
  const tableNames = [];

  buffer = await getNBytesOfDb(
    databaseFilePath,
    cellPointerArrayOffset,
    2 * cellCount,
    2 * cellCount
  );

  for (let i = 0; i < cellCount; i++) {
    const offset = buffer.readUInt16BE(i * 2);

    // console.log("CELL START OFFSET: ", offset);
    cellPointers.push(offset);

    let recordSize = 0,
      rowId = 0,
      recordHeaderSize = 0,
      serialTypeCode = 0,
      bytesRead = 0,
      totalBytesRead = 0;

    [recordSize, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    cellRecordSizes.push(recordSize);
    // console.log("RECORD SIZE LENGTH: ", bytesRead);

    [rowId, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    cellRowIds.push(rowId);
    // console.log("RECORD ROW IDs LENGTH: ", bytesRead);

    let contentStartOffset = offset + totalBytesRead;

    // console.log("RECORD HEADER START OFFSET: ", offset + totalBytesRead);
    [recordHeaderSize, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    cellRecordHeaderSizes.push(recordHeaderSize);
    // console.log("RECORD HEADER SIZE: ", recordHeaderSize);
    // console.log("RECORD HEADER SIZE LENGTH: ", bytesRead);
    contentStartOffset += recordHeaderSize;

    let contentBytesRead = 0;

    // TYPE
    let type = "";
    [serialTypeCode, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    [type, bytesRead] = await getRecordBody(
      databaseFilePath,
      contentStartOffset + contentBytesRead,
      serialTypeCode
    );
    contentBytesRead += bytesRead;
    // console.log("TYPE OF RECORD: ", type);

    // NAME
    let name = "";
    [serialTypeCode, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    [name, bytesRead] = await getRecordBody(
      databaseFilePath,
      contentStartOffset + contentBytesRead,
      serialTypeCode
    );
    contentBytesRead += bytesRead;

    // TABLE NAMES
    let tableName = "";
    [serialTypeCode, bytesRead] = await readVarInt(
      databaseFilePath,
      offset + totalBytesRead
    );
    totalBytesRead += bytesRead;
    [tableName, bytesRead] = await getRecordBody(
      databaseFilePath,
      contentStartOffset + contentBytesRead,
      serialTypeCode
    );
    contentBytesRead += bytesRead;
    tableNames.push(tableName.trim());

    // console.log("SERIAL TYPE CODE: ", serialTypeCode);
    // console.log("SERIAL TYPE CODE LENGTH: ", bytesRead);
    // console.log("RECORD HEADER END OFFSET: ", offset + totalBytesRead);
  }
  console.log(tableNames.join(" "));
} else {
  throw `Unknown command ${command}`;
}

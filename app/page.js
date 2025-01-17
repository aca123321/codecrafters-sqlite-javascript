import { DB_FILE_HEADER_OFFSET, DB_METADATA, getNBytesOfDb } from "./main.js";

export const getPageBuffer = async (pageNo) => {
  return await getNBytesOfDb(
    (pageNo - 1) * DB_METADATA.pageSize,
    DB_METADATA.pageSize,
    DB_METADATA.pageSize
  );
};

export const getPageHeader = async (pageBuffer, headerEndOffset = 0) => {
  const pageType = pageBuffer.readUInt8(headerEndOffset);
  const cellCount = pageBuffer.readUInt16BE(headerEndOffset + 3);
  return { pageType, cellCount };
};

const getCellPointers = async (
  pageBuffer,
  cellPointerArrayOffset,
  cellCount
) => {
  const cellPointers = [];
  for (let i = 0; i < cellCount; i++) {
    cellPointers.push(pageBuffer.readUInt16BE(cellPointerArrayOffset + 2 * i));
  }
  return cellPointers;
};

const readVarint = async (pageBuffer, offset) => {
  let val = 0;
  let toContinue = true;
  let bytesRead = 0;

  while (toContinue) {
    const byte = pageBuffer.readUInt8(offset + bytesRead);
    bytesRead++;

    toContinue = byte >= 128;
    val += byte + (toContinue ? -128 : 0);
  }
  return [val, offset + bytesRead];
};

const getRecordHeader = async (pageBuffer, offset) => {
  let curOffset = offset;
  let recordHeaderSize = 0;
  [recordHeaderSize, curOffset] = await readVarint(pageBuffer, curOffset);

  const serialTypeCodes = [];
  let serialTypeCode = 0;
  while (curOffset < offset + recordHeaderSize) {
    [serialTypeCode, curOffset] = await readVarint(pageBuffer, curOffset);
    serialTypeCodes.push(serialTypeCode);
  }

  return { recordHeaderSize, serialTypeCodes };
};

const getCols = async (pageBuffer, offset, serialTypeCodes) => {
  const cols = [];
  let contentOffset = offset;

  for (let [i, serialTypeCode] of serialTypeCodes.entries()) {
    let contentSize = 0;
    let content;

    if (serialTypeCode === 0) {
      content = null;
    } else if (serialTypeCode <= 6) {
      // ints
      if (serialTypeCode <= 4) {
        contentSize = serialTypeCode;
      } else if (serialTypeCode === 5) {
        contentSize = 6;
      } else {
        contentSize = 8;
      }

      if (serialTypeCode === 1) {
        content = pageBuffer.readUInt8(contentOffset);
      } else {
        content = pageBuffer.readUintBE(contentOffset, contentSize);
      }
    } else if (serialTypeCode === 7) {
      // float
      contentSize = 8;
      content = pageBuffer.readFloatBE(contentOffset);
    } else if (serialTypeCode <= 9) {
      content = serialTypeCode - 8;
    } else if (serialTypeCode >= 12) {
      if (serialTypeCode % 2 === 0) {
        contentSize = (serialTypeCode - 12) / 2;
        const tempBuffer = pageBuffer.subarray(
          contentOffset,
          contentOffset + contentSize
        );
        content = tempBuffer;
      } else {
        contentSize = (serialTypeCode - 13) / 2;
        content = pageBuffer
          .subarray(contentOffset, contentOffset + contentSize)
          .toString();
      }
    }

    cols.push(content);
    contentOffset += contentSize;
  }
  return cols;
};

const getRecord = async (pageBuffer, offset) => {
  const recordHeader = await getRecordHeader(pageBuffer, offset);
  const actualRecordStartOffset = offset + recordHeader.recordHeaderSize;

  const cols = await getCols(
    pageBuffer,
    actualRecordStartOffset,
    recordHeader.serialTypeCodes
  );

  return { recordHeader, cols };
};

export const getCells = async (pageBuffer, headerEndOffset = 0) => {
  const cells = [];

  const pageHeader = await getPageHeader(pageBuffer, headerEndOffset);
  if (!(pageHeader.pageType === 10 || pageHeader.pageType === 13)) {
    console.error("Page type not supported");
    return;
  }
  const cellPointerArrayOffset = headerEndOffset + 8;

  const cellPointers = await getCellPointers(
    pageBuffer,
    cellPointerArrayOffset,
    pageHeader.cellCount
  );

  for (let i = 0; i < pageHeader.cellCount; i++) {
    let curOffset = cellPointers[i];
    let recordSize = 0,
      rowId = 0;

    [recordSize, curOffset] = await readVarint(pageBuffer, curOffset);
    [rowId, curOffset] = await readVarint(pageBuffer, curOffset);

    const record = await getRecord(pageBuffer, curOffset, headerEndOffset != 0);
    cells.push({ recordSize, rowId, record });
  }

  return cells;
};

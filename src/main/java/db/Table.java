package db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import processor.parser.ast.Column;

public class Table {

  private final String tableName;
  private final int rootPage;
  private final String sqlStmt;
  private ByteBuffer pageBuffer;


  public Table(ByteBuffer cellBuffer) {
    // Cell
    int payloadSize = readVarInt(cellBuffer);
    int rowid = readVarInt(cellBuffer);
    // Payload
    // Payload Header
    int payloadHeaderSize = readVarInt(cellBuffer);
    int typeSize = getSizeFromSerialType(readVarInt(cellBuffer));
    int nameSize = getSizeFromSerialType(readVarInt(cellBuffer));
    int tableNameSize = getSizeFromSerialType(readVarInt(cellBuffer));
    int rootPageSize = getSizeFromSerialType(readVarInt(cellBuffer));
    int sqlStmtSize = getSizeFromSerialType(readVarInt(cellBuffer));

    // Record body starts here
    // Skip sqlite_schema.type from body
    byte[] typeBytes = new byte[typeSize];
    cellBuffer.get(typeBytes);
    // Skip sqlite_schema.name
    byte[] nameBytes = new byte[nameSize];
    cellBuffer.get(nameBytes);

    // Get table name sqlite_schema.tbl_name
    byte[] tableNameBytes = new byte[tableNameSize];
    cellBuffer.get(tableNameBytes);
    this.tableName = new String(tableNameBytes);

    // Get rootpage
    byte[] rootPageBytes = new byte[rootPageSize];
    cellBuffer.get(rootPageBytes);
    this.rootPage = getRootPage(rootPageBytes);

    // Get SQL `create` statement
    byte[] sqlStmtBytes = new byte[sqlStmtSize];
    cellBuffer.get(sqlStmtBytes);
    this.sqlStmt = new String(sqlStmtBytes);
  }

  public void setTablePageBuffer(ByteBuffer pageBuffer) {
    this.pageBuffer = pageBuffer.duplicate().clear().asReadOnlyBuffer();
  }

  public int getRows() throws IOException {
    int rowsPosition = 3;
    if (Objects.nonNull(this.pageBuffer) && this.pageBuffer.limit() > rowsPosition) {
      return Short.toUnsignedInt(this.pageBuffer.position(rowsPosition).getShort());
    } else {
      throw new IOException("Page size is less than required " + rowsPosition);
    }
  }

  public List<String> getAllByColumn(Column column, int order) throws IOException {
    int cellPointerArrayPosition = 8;
    if (Objects.nonNull(this.pageBuffer) && this.pageBuffer.limit() > cellPointerArrayPosition) {
      int rows = this.getRows();
      pageBuffer.position(cellPointerArrayPosition);
      ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(), 2 * rows);
      short[] offsets = new short[rows];
      List<String> values = new ArrayList<>();
      for (int i = 0; i < rows; i++) {
        // The last table offset goes the first
        offsets[i] = cellPointerArrayBuffer.getShort();
        ByteBuffer cellBuffer;
        if (i == 0) {
          cellBuffer = pageBuffer.position(offsets[i]).slice();
        } else {
          cellBuffer = pageBuffer.slice(offsets[i], offsets[i - 1] - offsets[i]);
        }
        // Cell
        int payloadSize = readVarInt(cellBuffer);
        int rowid = readVarInt(cellBuffer);
        // Payload
        // Payload Header
        int payloadHeaderSize = readVarInt(cellBuffer);
        int it = 0;
        while (cellBuffer.position() < payloadHeaderSize && it < order) {
//          int val = getSizeFromSerialType();

        }
      }
    } else {
      throw new IOException("Page size is less than required " + cellPointerArrayPosition);
    }

    return List.of();
  }

  public String getTableName() {
    return this.tableName;
  }

  public int getRootPage() {
    return this.rootPage;
  }

  public String getSqlStmt() {
    return this.sqlStmt;
  }

  private int getSizeFromSerialType(int serialType) {
    if (serialType > 13 && (serialType % 2 == 0)) {
      return (serialType - 13) / 2;
    } else if (serialType > 12) {
      return (serialType - 12) / 2;
    }
    return serialType;
  }

  private int getRootPage(byte[] rootPageBytes) {
    return switch (rootPageBytes.length) {
      case 1 -> Byte.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).get());
      case 2 -> Short.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).getShort());
      case 4 -> ByteBuffer.wrap(rootPageBytes).getInt();
      default -> throw new IllegalStateException("Rootpage couldn't be cast to integer type");
    };
  }

  private int readVarInt(byte[] data) {
    int value = 0;

    for (int i = 0; i < 9; i++) {
      int b = data[i] & 0xFF;

      // For the first 8 bytes:
      if (i < 8) {
        value = (value << 7) | (b & 0x7F);
        if ((b & 0x80) == 0) {
          // MSB is 0 => last byte
          return value;
        }
      } else {
        // 9th byte: uses all 8 bits
        value = (value << 8) | b;
        return value;
      }
    }
    throw new IllegalArgumentException("Value too large for unsigned varint");
  }

  // Warning! This method has side effects. Refactor it to immutability of cellBuffer
  private int readVarInt(ByteBuffer cellBuffer) {
    int result = 0;
    for (int i = 0; i < 8; i++) {
      final byte current = cellBuffer.get();
      result = (result << 7) + (current & 0x7F);
      if ((current & 0x80) == 0) {
        return result;
      }
    }
    final byte last = cellBuffer.get();
    result = (result << 8) + last;
    return result;
  }

}

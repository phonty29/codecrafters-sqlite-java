package db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class Table {

  private final String tableName;
  private final int rootPage;
  private final String sqlStmt;
  private ByteBuffer pageBuffer;


  public Table(ByteBuffer recordBuffer) {
    byte payloadSize = recordBuffer.get();
    // Skip rowid
    recordBuffer.get();
    // Payload starts here
    // Payload Header
    byte payloadHeaderSize = recordBuffer.get();
    byte typeSize = (byte) getSizeFromSerialType(recordBuffer.get());
    byte nameSize = (byte) getSizeFromSerialType(recordBuffer.get());
    byte tableNameSize = (byte) getSizeFromSerialType(recordBuffer.get());
    int rootPageSize = recordBuffer.get();
    byte[] sqlStmtSizeBytes = new byte[payloadHeaderSize - 5];
    recordBuffer.get(sqlStmtSizeBytes);
    int sqlStmtSize = getSizeFromSerialType(readUnsignedVarInt(sqlStmtSizeBytes));

    // Record body starts here
    // Skip sqlite_schema.type from body
    byte[] typeBytes = new byte[typeSize];
    recordBuffer.get(typeBytes);
    // Skip sqlite_schema.name
    byte[] nameBytes = new byte[nameSize];
    recordBuffer.get(nameBytes);

    // Get table name sqlite_schema.tbl_name
    byte[] tableNameBytes = new byte[tableNameSize];
    recordBuffer.get(tableNameBytes);
    this.tableName = new String(tableNameBytes);

    // Get rootpage
    byte[] rootPageBytes = new byte[rootPageSize];
    recordBuffer.get(rootPageBytes);
    this.rootPage = getRootPage(rootPageBytes);

    // Get SQL `create` statement
    byte[] sqlStmtBytes = new byte[sqlStmtSize];
    recordBuffer.get(sqlStmtBytes);
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
    } else {
      return (serialType - 12) / 2;
    }
  }

  private int getRootPage(byte[] rootPageBytes) {
    return switch (rootPageBytes.length) {
      case 1 -> Byte.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).get());
      case 2 -> Short.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).getShort());
      case 4 -> ByteBuffer.wrap(rootPageBytes).getInt();
      default -> throw new IllegalStateException("Rootpage couldn't be cast to integer type");
    };
  }

  private int readUnsignedVarInt(byte[] data) {
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
}

package db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class Table {
  private final String tableName;
  private final int rootPage;
  private ByteBuffer pageBuffer;


  public Table(ByteBuffer recordBuffer) {
    byte payloadSize = recordBuffer.get();
    // Skip rowid
    recordBuffer.get();
    // Payload starts here
    // Payload Header
    byte payloadHeaderSize = recordBuffer.get();
    byte typeSize = getSizeFromSerialType(recordBuffer.get());
    byte nameSize = getSizeFromSerialType(recordBuffer.get());
    byte tableNameSize = getSizeFromSerialType(recordBuffer.get());
    int rootPageSize = recordBuffer.get();
    // Skip rest record header
    recordBuffer.get(new byte[payloadHeaderSize - 5]);

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
    // REFACTOR! Let's assume for now that the rootpage size is 1 byte
    this.rootPage = recordBuffer.get();
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

  private byte getSizeFromSerialType(byte serialType) {
    if (serialType > 13 && (serialType%2 == 0)) {
      return (byte) ((serialType - 13) / 2);
    } else {
      return (byte) ((serialType - 12) / 2);
    }
  }
}

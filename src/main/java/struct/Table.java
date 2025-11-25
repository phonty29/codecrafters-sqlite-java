package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Table {

  private final String tableName;
  private final int rootPage;
  private final String sqlStmt;
  private ByteBuffer pageBuffer;


  public Table(ByteBuffer cellBuffer) {
    // Cell
    int payloadSize = readVarInt(cellBuffer); //not used
    int rowid = readVarInt(cellBuffer); // not used
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

  public List<String> getAllByColumn(int order) throws IOException {
    List<String> values = new ArrayList<>();
    int cellPointerArrayPosition = 8;
    if (Objects.nonNull(this.pageBuffer) && this.pageBuffer.limit() > cellPointerArrayPosition) {
      int rows = this.getRows();
      pageBuffer.position(cellPointerArrayPosition);
      ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(), 2 * rows);
      short[] offsets = new short[rows];
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
        int[] sizes = new int[order];
        int currentPosition = cellBuffer.position();
        // Until payloadHeaderSize - 1, because payload header is self included
        while (cellBuffer.position() - currentPosition < payloadHeaderSize - 1) {
          int size = getSizeFromSerialType(readVarInt(cellBuffer));
          if (it < order) {
            sizes[it++]= size;
          }
        }
        // Skip
        cellBuffer.position(cellBuffer.position());
        for (int j = 0; j < order-1; j++) {
          byte[] valueBytes = new byte[sizes[j]];
          cellBuffer.get(valueBytes);
        }
        // Read value
        byte[] valueBytes = new byte[sizes[order-1]];
        cellBuffer.get(valueBytes);
        values.add(new String(valueBytes));
      }
    } else {
      throw new IOException("Page size is less than required " + cellPointerArrayPosition);
    }
    return values;
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
    if (serialType >= 13 && (serialType % 2 == 1)) {
      return (serialType - 13) / 2;
    } else if (serialType >= 12) {
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

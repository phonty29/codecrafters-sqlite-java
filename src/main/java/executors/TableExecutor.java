package executors;

import db.DbFile;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TableExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      DbFile dbFile = new DbFile(databaseFile);
      // Skip till cell pointer array
      ByteBuffer pageBuffer = dbFile.getPageBuffer();
      // Set position to after the "number of cells on the page"
      pageBuffer.position(105);

      int cellHeaderTailLength = 3;
      pageBuffer.position(pageBuffer.position() + cellHeaderTailLength);
      // Slice Cell Pointer Array as buffer
      int numberOfTables = dbFile.getNumberOfTables();
      ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(), 2*numberOfTables);

      String[] tableNames = new String[numberOfTables];
      short[] offsets = new short[numberOfTables];
      for (int i = 0; i < numberOfTables; i++) {
        // The last table offset goes first, so printing order from last-to-first
        offsets[i] = cellPointerArrayBuffer.getShort();
        if (i == 0) {
          tableNames[i] = getTableNameFromRecord(pageBuffer.position(offsets[i]).slice());
        } else {
          tableNames[i] = getTableNameFromRecord(pageBuffer.slice(offsets[i], offsets[i-1] - offsets[i]));
        }
      }

      System.out.println(String.join(" ", tableNames));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }

  private String getTableNameFromRecord(ByteBuffer recordBuffer) {
    byte payloadSize = recordBuffer.get();
    // Skip rowid
    recordBuffer.get();
    // Payload starts here
    byte payloadHeaderSize = recordBuffer.get();
    byte typeSize = getSizeFromSerialType(recordBuffer.get());
    byte nameSize = getSizeFromSerialType(recordBuffer.get());
    byte tableNameSize = getSizeFromSerialType(recordBuffer.get());
    // Skip rest record header
    recordBuffer.get(new byte[payloadHeaderSize - 4]);
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
    return new String(tableNameBytes);
  }

  private byte getSizeFromSerialType(byte serialType) {
    if (serialType > 13 && (serialType%2 == 0)) {
      return (byte) ((serialType - 13) / 2);
    } else {
      return (byte) ((serialType - 12) / 2);
    }
  }
}

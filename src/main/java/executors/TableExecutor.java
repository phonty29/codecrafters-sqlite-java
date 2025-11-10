package executors;

import static executors.Utils.getSize;
import static executors.Utils.validateBTreePageType;

import exceptions.IncorrectBTreePageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;

public class TableExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      FileChannel channel = databaseFile.getChannel();
      channel.position(16);
      // Obtain page size
      ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
      channel.read(pageSizeBuffer);
      int pageSize = Short.toUnsignedInt(pageSizeBuffer.duplicate().clear().getShort());
      // Copy first page to buffer
      ByteBuffer pageBuffer = ByteBuffer.allocate(pageSize);
      channel.position(0).read(pageBuffer);
      // Get number of cells in sqlite_schema
      short numberOfTables = pageBuffer.position(103).getShort();

      // Skip till cell pointer array
      int cellHeaderTailLength = 3;
      pageBuffer.position(pageBuffer.position() + cellHeaderTailLength);
      // Slice Cell Pointer Array as buffer
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
    System.out.println("RecordBuffer capacity: " + recordBuffer.capacity());
    System.out.println("RecordBuffer limit: " + recordBuffer.limit());
    byte payloadSize = recordBuffer.get();
    // Skip rowid
    recordBuffer.get();
    // Payload starts here
    byte payloadHeaderSize = recordBuffer.get();
    byte typeSize = getSize(recordBuffer.get());
    byte nameSize = getSize(recordBuffer.get());
    byte tableNameSize = getSize(recordBuffer.get());
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
}

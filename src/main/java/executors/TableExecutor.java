package executors;

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
      for (int i = 0; i < numberOfTables; i++) {
        // The last table offset goes first, so printing order from last-to-first

      }


//      String[] tableNames = Utils.getTableNamesFromRecords(tableData, tableOffsets.length);
//      tableNames[tableNames.length-1] = Utils.getLastTableNameFromFile(databaseFile);

      Collections.reverse(Arrays.asList(tableNames));

      System.out.println(String.join(" ", tableNames));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

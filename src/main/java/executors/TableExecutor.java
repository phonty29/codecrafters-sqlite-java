package executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class TableExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try {
      FileInputStream databaseFile = new FileInputStream(filePath);

      databaseFile.skip(103); // Skip the first 16 bytes of the header
      byte[] numberOfTablesBytes = new byte[2];
      databaseFile.read(numberOfTablesBytes);
      short numberOfTables = ByteBuffer.wrap(numberOfTablesBytes).getShort();
      // Skip until cell pointer array
      databaseFile.skip(3);
      byte[] cellPointerArrayBytes = new byte[2*numberOfTables];
      databaseFile.read(cellPointerArrayBytes);

      ByteBuffer cellPointerArray = ByteBuffer.wrap(cellPointerArrayBytes);
      short[] tableOffsets = new short[numberOfTables];
      for (int i = 0; i < numberOfTables; i++) {
        // The last table offset goes first, so printing order from last-to-first
        tableOffsets[i] = cellPointerArray.getShort();
      }

      int lastTableOffset = tableOffsets[0];
      int firstTableOffset = tableOffsets[numberOfTables-1];
      int tableDataSize = lastTableOffset - firstTableOffset;
      int skipToFirstOffset = firstTableOffset - (108 + (2*numberOfTables));
      databaseFile.skip(skipToFirstOffset);

      byte[] tableData = new byte[tableDataSize];
      databaseFile.read(tableData);

      String[] tableNames = Utils.getTableNamesFromRecords(tableData, tableOffsets.length);
      tableNames[tableNames.length-1] = Utils.getLastTableNameFromFile(databaseFile);

      Collections.reverse(Arrays.asList(tableNames));
      // You can use print statements as follows for debugging, they'll be visible when running tests.
      System.err.println("Logs from your program will appear here!");

      System.out.println(String.join(" ", tableNames));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

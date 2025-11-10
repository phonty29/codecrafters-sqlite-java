package executors;

import static executors.Utils.validateBTreePageType;

import exceptions.IncorrectBTreePageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class TableExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      // Skip the first 100 bytes = Database File Header
      int databaseFileHeaderLength = 100;
      databaseFile.skip(databaseFileHeaderLength);
      // Skip B-tree page type and freeblocks
      // B-tree page type:
      byte bTreePageType = (byte) databaseFile.read();
      validateBTreePageType(bTreePageType);

      // Skip freeblocks
      int freeblocksLength = 2;
      databaseFile.skip(freeblocksLength);

      // Get number of cells in sqlite_schema
      int numberOfTablesLength = 2;
      byte[] numberOfTablesBytes = new byte[numberOfTablesLength];
      databaseFile.read(numberOfTablesBytes);
      short numberOfTables = ByteBuffer.wrap(numberOfTablesBytes).getShort();
      // Skip till cell pointer array
      int cellHeaderTailLength = 3;
      databaseFile.skip(cellHeaderTailLength);

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

      databaseFile.getChannel();
      byte[] tableData = new byte[tableDataSize];
      databaseFile.read(tableData);

      String[] tableNames = Utils.getTableNamesFromRecords(tableData, tableOffsets.length);
      tableNames[tableNames.length-1] = Utils.getLastTableNameFromFile(databaseFile);

      Collections.reverse(Arrays.asList(tableNames));

      System.out.println(String.join(" ", tableNames));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

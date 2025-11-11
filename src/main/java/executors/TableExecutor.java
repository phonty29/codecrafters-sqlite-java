package executors;

import db.DbFile;
import db.Table;
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
          tableNames[i] = new Table(pageBuffer.position(offsets[i]).slice()).getTableName();
        } else {
          tableNames[i] = new Table(pageBuffer.slice(offsets[i], offsets[i-1] - offsets[i])).getTableName();
        }
      }

      System.out.println(String.join(" ", tableNames));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

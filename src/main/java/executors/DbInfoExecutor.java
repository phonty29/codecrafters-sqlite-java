package executors;

import exceptions.IncorrectBTreePageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DbInfoExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      databaseFile.skip(16); // Skip the first 16 bytes of the header [SQLite format]

      // Read page size
      byte[] pageSizeBytes = new byte[2];
      databaseFile.read(pageSizeBytes);
      short pageSizeSigned = ByteBuffer.wrap(pageSizeBytes).getShort();
      int pageSize = Short.toUnsignedInt(pageSizeSigned);

      // Skip till B-tree page header
      databaseFile.skip(100 - 18);
      // B-tree page type:
      byte bTreePageType = (byte) databaseFile.read();
      validateBTreePageType(bTreePageType);

      // Skip freeblocks
      databaseFile.skip(2);

      // Get number of cells in sqlite_schema
      byte[] numberOfTablesBytes = new byte[2];
      databaseFile.read(numberOfTablesBytes);
      short numberOfTables = ByteBuffer.wrap(numberOfTablesBytes).getShort();

      // Print results
      System.out.println("database page size: " + pageSize);
      System.out.println("number of tables: " + numberOfTables);
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }

  private void validateBTreePageType(byte type) throws IncorrectBTreePageType {
    if (type != 0x02 && type != 0x05 && type != 0x0a && type != 0x0d) {
      throw new IncorrectBTreePageType(type);
    }
  }
}

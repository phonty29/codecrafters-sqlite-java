package executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DbInfoExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try {
      FileInputStream databaseFile = new FileInputStream(filePath);

      databaseFile.skip(16); // Skip the first 16 bytes of the header
      byte[] pageSizeBytes = new byte[2]; // The following 2 bytes are the page size
      databaseFile.read(pageSizeBytes);
      short pageSizeSigned = ByteBuffer.wrap(pageSizeBytes).getShort();
      int pageSize = Short.toUnsignedInt(pageSizeSigned);
      // Skip until b-tree page header
      databaseFile.skip(100 - 18 + 3);

      byte[] numberOfTablesBytes = new byte[2];
      databaseFile.read(numberOfTablesBytes);
      short numberOfTables = ByteBuffer.wrap(numberOfTablesBytes).getShort();

      // You can use print statements as follows for debugging, they'll be visible when running tests.
      System.err.println("Logs from your program will appear here!");

      System.out.println("database page size: " + pageSize);
      System.out.println("number of tables: " + numberOfTables);
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

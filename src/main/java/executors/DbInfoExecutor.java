package executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DbInfoExecutor implements Executor {

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

      // Print results
      System.out.println("database page size: " + pageSize);
      System.out.println("number of tables: " + numberOfTables);
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

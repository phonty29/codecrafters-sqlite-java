package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DbFile {
  private final FileInputStream databaseFile;
  private final FileChannel channel;
  private final int pageSize;
  private final int numberOfTables;
  private final ByteBuffer pageBuffer;

  public DbFile(FileInputStream databaseFile) throws IOException {
    this.databaseFile = databaseFile;
    this.channel = databaseFile.getChannel();
    this.channel.position(16);
    // Obtain page size
    ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
    channel.read(pageSizeBuffer);
    this.pageSize = Short.toUnsignedInt(pageSizeBuffer.duplicate().clear().getShort());
    // Copy first page to buffer
    this.pageBuffer = ByteBuffer.allocate(pageSize);
    channel.position(0).read(pageBuffer);
    // Get number of cells in sqlite_schema
    this.numberOfTables = pageBuffer.position(103).getShort();
  }

  public FileInputStream getDbFileInputStream() {
    return this.databaseFile;
  }

  public FileChannel getDbFileChannel() {
    return this.channel;
  }

  public int getNumberOfTables() {
    return this.numberOfTables;
  }

  public int getPageSize() {
    return this.pageSize;
  }

  public ByteBuffer getPageBuffer() {
    return this.pageBuffer.duplicate().clear();
  }
}

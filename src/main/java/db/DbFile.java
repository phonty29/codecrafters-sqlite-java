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
  private ByteBuffer pageBuffer;
  private int currentPage = 1;

  public DbFile(FileInputStream databaseFile) throws IOException {
    this.databaseFile = databaseFile;
    this.channel = databaseFile.getChannel();
    this.channel.position(16);
    // Obtain page size
    ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
    channel.read(pageSizeBuffer);
    this.pageSize = Short.toUnsignedInt(pageSizeBuffer.duplicate().clear().getShort());
    setPage(this.currentPage);
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

  public void setPage(int page) throws IOException {
    // Copy page to buffer
    this.currentPage = page;
    this.pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (this.currentPage - 1) * this.pageSize).read(pageBuffer);
  }

  public ByteBuffer getPageBuffer() {
    return this.pageBuffer.duplicate().clear();
  }
}

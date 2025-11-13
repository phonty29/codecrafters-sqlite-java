package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Database {
  private final FileInputStream databaseFile;
  private final FileChannel channel;
  private final int pageSize;
  private final int numberOfTables;
  private ByteBuffer pageBuffer;
  private int currentPage = 1;
  private final Table[] tables;

  public Database(FileInputStream databaseFile) throws IOException {
    this.databaseFile = databaseFile;
    this.channel = databaseFile.getChannel();
    // Skip Magic numbers
    this.channel.position(16);

    // Get page size
    ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
    channel.read(pageSizeBuffer);
    this.pageSize = Short.toUnsignedInt(pageSizeBuffer.clear().getShort());
    setCurrentPage(this.currentPage);

    // Get number of cells in sqlite_schema
    this.numberOfTables = pageBuffer.position(103).getShort();

    // Set position to after the "number of cells on the page"
    pageBuffer.position(105);
    int cellHeaderTailLength = 3;
    pageBuffer.position(pageBuffer.position() + cellHeaderTailLength);
    // Slice Cell Pointer Array as buffer
    ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(), 2*numberOfTables);

    // Initialize tables
    this.tables = new Table[numberOfTables];
    short[] offsets = new short[numberOfTables];
    for (int i = 0; i < numberOfTables; i++) {
      // The last table offset goes first, so printing order from last-to-first
      offsets[i] = cellPointerArrayBuffer.getShort();
      if (i == 0) {
        tables[i] = new Table(pageBuffer.position(offsets[i]).slice());
      } else {
        tables[i] = new Table(pageBuffer.slice(offsets[i], offsets[i-1] - offsets[i]));
      }
    }
  }

  public int getNumberOfTables() {
    return this.numberOfTables;
  }

  public int getPageSize() {
    return this.pageSize;
  }

  public Table[] getTables() {
    return this.tables;
  }

  public void setCurrentPage(int page) throws IOException {
    // Copy page to buffer
    this.currentPage = page;
    this.pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (this.currentPage - 1) * this.pageSize).read(pageBuffer);
  }

  public ByteBuffer getCurrentPageBuffer() {
    return this.pageBuffer.duplicate().clear();
  }
}

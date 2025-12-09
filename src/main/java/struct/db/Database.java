package struct.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import struct.BTreePage;
import struct.Table;
import struct.cells.LeafTableCell;

public class Database {

  private final FileChannel channel;
  private final int pageSize;
  private final Table[] tables;
  private final BTreePage bTreePage;

  protected Database(FileInputStream databaseFile) throws IOException {
    this.channel = databaseFile.getChannel();
    // Read meta from database file headers (first 100 bytes)
    // Skip Magic numbers
    this.channel.position(16);
    // Get page size
    ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
    channel.read(pageSizeBuffer);
    this.pageSize = Short.toUnsignedInt(pageSizeBuffer.clear().getShort());
    // Copy page to buffer, then update b-tree page
    ByteBuffer pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position(0).read(pageBuffer);
    this.bTreePage = new BTreePage(pageBuffer.position(100));

    // Initialize tables with meta from sqlite_schema
    int numberOfTables = this.bTreePage.getPageHeader().cellsCount();
    this.tables = new Table[numberOfTables];
    LeafTableCell[] schemaCells = (LeafTableCell[]) this.bTreePage.getCells();
    for (int i = 0; i < schemaCells.length; i++) {
      tables[i] = new Table(schemaCells[i]);
    }
  }

  public int getNumberOfTables() {
    return this.bTreePage.getPageHeader().cellsCount();
  }

  public int getPageSize() {
    return this.pageSize;
  }

  public Table[] getTables() {
    return this.tables;
  }

  public Table getTable(String tableName) {
    return Arrays.stream(this.tables)
        .filter(t -> t.meta().name().contains(tableName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Required table not found: " + tableName));
  }

  public void navigateToTable(Table table) {
    try {
      ByteBuffer tablePageBuffer = ByteBuffer.allocate(this.pageSize);
      this.channel.position((long) (table.meta().rootPageNumber() - 1) * this.pageSize)
          .read(tablePageBuffer);
      table.setRootPage(new BTreePage(tablePageBuffer.duplicate().clear()));
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }

  public void navigateToPageOfTable(int pageNumber, Table table) {
    try {
      ByteBuffer pageBuffer = ByteBuffer.allocate(this.pageSize);
      this.channel.position((long) (pageNumber - 1) * this.pageSize)
          .read(pageBuffer);
      table.setCurrentPage(new BTreePage(pageBuffer.duplicate().clear()));
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }
}

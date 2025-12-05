package struct;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class Database {

  private final FileChannel channel;
  private final int pageSize;
  private final Table[] tables;
  private final BTreePage bTreePage;

  public Database(FileInputStream databaseFile) throws IOException {
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

    // Initialize tables
    int numberOfTables = this.bTreePage.getPageHeader().cellsCount();
    this.tables = new Table[numberOfTables];
    LeafTableCell[] tableLeafTableCells = this.bTreePage.getLeafCells();
    for (int i = 0; i < tableLeafTableCells.length; i++) {
      tables[i] = new Table(this, tableLeafTableCells[i]);
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

  // Add support for interior table pages
  public void navigateToTable(Table table) throws IOException {
    ByteBuffer tablePageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (table.meta().rootPage() - 1) * this.pageSize)
        .read(tablePageBuffer);
    table.setTablePage(new BTreePage(tablePageBuffer.duplicate().clear()));
  }

  public BTreePage getPage(int rootPage) throws IOException {
    ByteBuffer pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (rootPage - 1) * this.pageSize)
        .read(pageBuffer);
    return new BTreePage(pageBuffer.duplicate().clear());
  }
}

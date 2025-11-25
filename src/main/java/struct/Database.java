package struct;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Database {
  private final FileInputStream databaseFile;
  private final FileChannel channel;
  private final int pageSize;
  private final int numberOfTables;
  private final BTreePageType bTreePageType;
  private final Table[] tables;
  private ByteBuffer pageBuffer;
  private int currentPage = 1;
  private BTreePage currentBTreePage;

  public Database(FileInputStream databaseFile) throws IOException {
    this.databaseFile = databaseFile;
    this.channel = databaseFile.getChannel();
    // Read meta from database file headers (first 100 bytes)
    // Skip Magic numbers
    this.channel.position(16);
    // Get page size
    ByteBuffer pageSizeBuffer = ByteBuffer.allocate(2);
    channel.read(pageSizeBuffer);
    this.pageSize = Short.toUnsignedInt(pageSizeBuffer.clear().getShort());
    this.setCurrentPage(this.currentPage, 100);

    this.bTreePageType = this.currentBTreePage.getPageHeader().getPageType();
    this.numberOfTables = this.currentBTreePage.getPageHeader().getCellsCount();

    // Initialize tables
    this.tables = new Table[numberOfTables];
    var tableCells = this.currentBTreePage.getCells();
    for (int i = 0; i < tableCells.length; i++) {
      tables[i] = new Table(tableCells[i]);
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

  public void navigateToTable(Table table) throws IOException {
    this.currentPage = table.getRootPage();
    this.pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (table.getRootPage() - 1) * this.pageSize).read(pageBuffer);
    this.currentBTreePage = new BTreePage(pageBuffer.duplicate().position(0));
    table.setTablePage(currentBTreePage);
//    table.setTablePageBuffer(pageBuffer);
  }

  public void setCurrentPage(int rootPage, int offset) throws IOException {
    // Copy page to buffer, then update b-tree page
    this.currentPage = rootPage;
    this.pageBuffer = ByteBuffer.allocate(this.pageSize);
    this.channel.position((long) (rootPage - 1) * this.pageSize).read(pageBuffer);
    this.currentBTreePage = new BTreePage(pageBuffer.duplicate().position(offset));
  }

  public ByteBuffer getCurrentPageBuffer() {
    return this.pageBuffer.duplicate().clear();
  }

  public BTreePageType getBTreePageType() {
    return this.bTreePageType;
  }

  public BTreePage getCurrentBTreePage() {
    return this.currentBTreePage;
  }
}

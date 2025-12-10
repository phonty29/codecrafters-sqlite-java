package struct.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import struct.btree.BTreePage;
import struct.schema.Index;
import struct.schema.SchemaElement;
import struct.schema.SchemaType;
import struct.schema.Table;
import struct.cells.LeafTableCell;

public class Database {

  private final FileChannel channel;
  private final int pageSize;
  private final List<Table> tables = new ArrayList<>();
  private final List<Index> indexes = new ArrayList<>();
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
    LeafTableCell[] schemaCells = (LeafTableCell[]) this.bTreePage.getCells();
    for (LeafTableCell schemaCell : schemaCells) {
      switch (schemaType(schemaCell)) {
        case TABLE -> this.tables.add(new Table(schemaCell));
        case INDEX -> this.indexes.add(new Index(schemaCell));
      }
    }
  }

  private SchemaType schemaType(LeafTableCell cell) {
    String schemaType = new String(cell.getRecordBody().values()[0]);
    return SchemaType.fromName(schemaType);
  }

  public int getNumberOfTables() {
    return this.tables.size();
  }

  public int getNumberOfIndexes() {
    return this.indexes.size();
  }

  public int getPageSize() {
    return this.pageSize;
  }

  public List<Table> getTables() {
    return this.tables;
  }

  public List<Index> getIndexes() {
    return this.indexes;
  }

  public Table getTable(String tableName) {
    return this.tables.stream()
        .filter(t -> t.meta().name().contains(tableName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Required table not found: " + tableName));
  }

  public void navigateTo(SchemaElement element) {
    try {
      ByteBuffer pageBuffer = ByteBuffer.allocate(this.pageSize);
      this.channel.position((long) (element.getRootPageNumber() - 1) * this.pageSize)
          .read(pageBuffer);
      element.setRootPage(new BTreePage(pageBuffer.duplicate().clear()));
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

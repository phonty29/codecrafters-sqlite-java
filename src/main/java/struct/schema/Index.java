package struct.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import query_processor.SqlProcessor;
import struct.btree.BTreePage;
import struct.btree.BTreePageType;
import struct.cells.LeafTableCell;

public class Index implements SchemaElement {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final List<String> columns = new ArrayList<>();
  private BTreePage currentPage;
  private BTreePage rootPage;

  public Index(LeafTableCell cell) {
    // Get meta from sqlite_schema cells
    int indexNameOrder = 1;
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int createStmtOrder = 4;
    byte[][] cellValues = cell.getRecordBody().values();
    String indexName = new String(cellValues[indexNameOrder]);
    String tableName = new String(cellValues[tableNameOrder]);
    int rootPageNumber = getRootPage(cellValues[rootPageOrder]);
    String sqlStmt = new String(cellValues[createStmtOrder]);
    this.meta = new Meta(indexName, tableName, rootPageNumber, sqlStmt);

    // Process `create index` query
    this.sqlProcessor = new SqlProcessor(sqlStmt);
    this.columns.addAll(this.sqlProcessor.getIndexedColumns());
  }

  public List<String> getColumns() {
    return this.columns;
  }

  @Override
  public void setCurrentPage(BTreePage currentPage) {
    validatePage(currentPage);
    this.currentPage = currentPage;
  }

  @Override
  public void setRootPage(BTreePage rootPage) {
    // Root page can be set only once
    if (Objects.isNull(this.rootPage)) {
      validatePage(rootPage);
      this.rootPage = rootPage;
      this.currentPage = rootPage;
    } else {
      throw new IllegalArgumentException("Cannot change root page");
    }
  }

  private void validatePage(BTreePage page) {
    if (Objects.isNull(page) || (!page.getPageHeader().pageType().equals(BTreePageType.INT_INDEX)
        && !page.getPageHeader().pageType().equals(BTreePageType.LEAF_INDEX))) {
      throw new IllegalArgumentException("Invalid page type for index");
    }
  }

  @Override
  public int getRootPageNumber() {
    return this.meta.rootPageNumber();
  }

  public Meta meta() {
    return this.meta;
  }

  public record Meta(
      String name,
      String tableName,
      int rootPageNumber,
      String createStmt
  ) {

  }
}
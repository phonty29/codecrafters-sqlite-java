package storage.struct;

import java.util.Objects;
import qprocessor.scanners.TableScanner;
import storage.btree.BTreePage;
import storage.btree.BTreePageType;
import storage.cells.LeafTableCell;

public class Table implements Structure {

  private final Meta meta;
  private BTreePage rootPage;
  private BTreePage currentPage;

  public Table(LeafTableCell schema) {
    // Get meta from sqlite_schema cells
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int sqlStmtOrder = 4;
    byte[][] cellValues = schema.getRecordBody().values();
    String tableName = new String(cellValues[tableNameOrder]);
    int rootPageNumber = getRootPage(cellValues[rootPageOrder]);
    String sqlStmt = new String(cellValues[sqlStmtOrder]);
    this.meta = new Meta(tableName, rootPageNumber, sqlStmt);
  }

  public TableScanner scanner() {
    return new TableScanner(this);
  }

  @Override
  public BTreePage getCurrentPage() {
    return this.currentPage;
  }

  @Override
  public void setCurrentPage(BTreePage currentPage) {
    validatePage(currentPage);
    this.currentPage = currentPage;
  }

  @Override
  public BTreePage getRootPage() {
    return this.rootPage;
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

  @Override
  public int getRootPageNumber() {
    return this.meta.rootPageNumber();
  }

  private void validatePage(BTreePage page) {
    if (Objects.isNull(page) || (!page.getPageHeader().pageType().equals(BTreePageType.INT_TABLE)
        && !page.getPageHeader().pageType().equals(BTreePageType.LEAF_TABLE))) {
      throw new IllegalArgumentException("Invalid page type for table");
    }
  }

  public int getCellsCount() {
    if (Objects.nonNull(this.rootPage) && this.rootPage.getPageHeader().pageType()
        .equals(BTreePageType.LEAF_TABLE)) {
      return this.rootPage.getPageHeader().cellsCount();
    }
    throw new IllegalStateException("Root page is not a leaf table");
  }

  public Meta meta() {
    return this.meta;
  }

  public record Meta(
      String name,
      int rootPageNumber,
      String sqlStmt
  ) {

  }
}

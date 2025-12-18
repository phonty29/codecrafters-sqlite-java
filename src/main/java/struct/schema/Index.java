package struct.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import query_processor.SqlProcessor;
import struct.btree.BTreePage;
import struct.btree.BTreePageType;
import struct.cells.IndexCell;
import struct.cells.InteriorIndexCell;
import struct.cells.LeafIndexCell;
import struct.cells.LeafTableCell;
import struct.db.DatabaseProducer;
import utils.ByteUtils;

public class Index implements SchemaElement {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final String column;
  private BTreePage currentPage;
  private BTreePage rootPage;
  private String searchValue;

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
    this.column = this.sqlProcessor.getIndexedColumns().getFirst();
  }

  public void setSearchValue(String searchValue) {
    this.searchValue = searchValue;
  }

  public List<Row> get() {
    return switch (this.currentPage.getPageHeader().pageType()) {
      case INT_INDEX -> getFromInteriorPages();
      case LEAF_INDEX -> getFromLeafPages();
      default -> throw new IllegalStateException("Unexpected page type for index: " + this.rootPage.getPageHeader().pageType());
    };
  }

  private List<Row> getFromInteriorPages() {
    if (!(this.currentPage.getCells() instanceof InteriorIndexCell[])) {
      throw new IllegalStateException("Current page is not an interior index page");
    }

    BTreePage interiorPage = this.currentPage;
    List<Row> rows = new ArrayList<>();
    Arrays.stream((InteriorIndexCell[]) this.currentPage.getCells()).forEach(cell -> {
      var row = formatIndexRow(cell);
      if (matchesFilters(row)) {
        rows.add(row);
      }
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
      rows.addAll(this.get());
    });
    DatabaseProducer.get().navigateToPageOfElement(interiorPage.getRightmostPointer(), this);
    rows.addAll(this.get());
    return rows;
  }

  private List<Row> getFromLeafPages() {
    if (!(this.currentPage.getCells() instanceof LeafIndexCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf index page");
    }

    return Arrays
        .stream(leafCells)
        .map(this::formatIndexRow)
        .filter(this::matchesFilters)
        .toList();
  }

  private Row formatIndexRow(IndexCell cell) {
    var values = new HashMap<String, String>();
    for (int i = 0; i < cell.getRecordBody().values().length - 1; i++) {
      values.put(this.column, new String(cell.getRecordBody().values()[i]));
    }
    int rowId = ByteUtils.toInteger(cell.getRecordBody().values()[cell.getRecordBody().values().length - 1]).intValue();
    return new Row(rowId, values);
  }

  private boolean matchesFilters(Row row) {
    return row.values.get(this.column).contentEquals(this.searchValue);
  }

  public String getColumn() {
    return this.column;
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

  public record Row(
      int rowId,
      Map<String, String> values
  ) {

  }
}
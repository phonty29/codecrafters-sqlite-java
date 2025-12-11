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
import struct.cells.InteriorIndexCell;
import struct.cells.LeafIndexCell;
import struct.cells.LeafTableCell;
import struct.db.DatabaseProducer;
import utils.ByteUtils;

public class Index implements SchemaElement {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final List<String> columns = new ArrayList<>();
  private final List<Row> rows = new ArrayList<>();
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

  public List<Row> iterate() {
    return switch (this.currentPage.getPageHeader().pageType()) {
      case INT_INDEX -> iterateInteriorPages();
      case LEAF_INDEX -> iterateLeafPages();
      default -> throw new IllegalStateException("Unexpected page type for index: " + this.rootPage.getPageHeader().pageType());
    };
  }

  private List<Row> iterateInteriorPages() {
    if (!(this.currentPage.getCells() instanceof InteriorIndexCell[])) {
      throw new IllegalStateException("Current page is not an interior index page");
    }

    BTreePage interiorPage = this.currentPage;
    Arrays.stream((InteriorIndexCell[]) this.currentPage.getCells()).forEach(cell -> {
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
      this.iterate();
    });
    DatabaseProducer.get().navigateToPageOfElement(interiorPage.getRightmostPointer(), this);
    this.iterate();
    return this.rows;
  }

  private List<Row> iterateLeafPages() {
    if (!(this.currentPage.getCells() instanceof LeafIndexCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf index page");
    }

    return Arrays
        .stream(leafCells)
        .map(this::formatIndexRow)
        .toList();
  }

  private Row formatIndexRow(LeafIndexCell cell) {
    if (cell.getRecordBody().values().length - 1 != this.columns.size()) {
      throw new IllegalStateException("Indexed columns do not match: " + this.columns);
    }
    var values = new HashMap<String, String>();
    for (int i = 0; i < cell.getRecordBody().values().length - 1; i++) {
      values.put(this.columns.get(i), new String(cell.getRecordBody().values()[i]));
    }
    int rowId = ByteUtils.toInteger(cell.getRecordBody().values()[cell.getRecordBody().values().length - 1]).intValue();
    var row = new Row(rowId, values);
    if (row.values.get("country").contentEquals("kazakhstan")) {
      System.out.println("Row: " + row);
    }
    return row;
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

  public record Row(
      int rowId,
      Map<String, String> values
  ) {

  }
}
package storage.struct;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import processing.Row;
import processing.planner.QueryPlanner;
import processing.query.QueryEngine;
import processing.query.parser.ast.Column;
import processing.query.parser.ast.ColumnType;
import processing.scanners.IndexScanner;
import storage.btree.BTreePage;
import storage.btree.BTreePageType;
import storage.cells.InteriorTableCell;
import storage.cells.LeafTableCell;
import storage.db.DatabaseProducer;
import utils.ByteUtils;

public class Table implements Structure {

  private final Meta meta;
  private final QueryEngine queryEngine;
  private final Column[] columns;
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

    // Init table structure
    this.queryEngine = new QueryEngine(this.meta.sqlStmt);
    this.columns = queryEngine.getColumns();
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

  @Override
  public BTreePage getCurrentPage() {
    return this.currentPage;
  }

  @Override
  public BTreePage getRootPage() {
    return this.rootPage;
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

  public List<String> scan(List<String> queriedColumns, QueryPlanner planner) {
    return switch (planner.scanType()) {
      case INDEX_SCAN -> {
        List<Integer> rowIds = planner.indexScanners().stream().map(IndexScanner::scan)
            .flatMap(List::stream).toList();
        yield this.scanWithIndex(queriedColumns, rowIds);
      }
      case TABLE_SCAN -> switch (this.rootPage.getPageHeader().pageType()) {
        case LEAF_TABLE -> scanLeaf(queriedColumns, planner.filter());
        case INT_TABLE -> scanInterior(queriedColumns, planner.filter());
        default -> throw new IllegalStateException(
            "Not supported page type: " + this.currentPage.getPageHeader().pageType());
      };
    };
  }

  public List<String> scanWithIndex(List<String> queriedColumns, List<Integer> rowIds) {
    List<String> values = new ArrayList<>();
    BTreePage parentPage = this.currentPage;
    rowIds.forEach(rowId -> {
      this.currentPage = parentPage;
      values.add(this.scanByRowId(rowId, queriedColumns));
    });
    return values;
  }

  public String scanByRowId(Integer rowId, List<String> queriedColumns) {
    return switch (this.currentPage.getPageHeader().pageType()) {
      case INT_TABLE -> scanInteriorByRowId(rowId, queriedColumns);
      case LEAF_TABLE -> scanLeafByRowId(rowId, queriedColumns);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  private String scanInteriorByRowId(Integer rowId, List<String> queriedColumns) {
    if (!(currentPage.getCells() instanceof InteriorTableCell[] interiorTableCells)) {
      throw new IllegalStateException("Current page is not an interior table");
    }

    BTreePage parentPage = this.currentPage;
    for (var cell : interiorTableCells) {
      if (rowId <= cell.getRowId()) {
        DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
        return this.scanByRowId(rowId, queriedColumns);
      }
    }
    DatabaseProducer.get().navigateToPageOfElement(parentPage.getRightmostPointer(), this);
    return this.scanByRowId(rowId, queriedColumns);
  }

  private String scanLeafByRowId(Integer rowId, List<String> queriedColumns) {
    if (!(currentPage.getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    for (var cell : leafCells) {
      if (rowId == cell.getRowId()) {
        return formatRowColumns(toRow(cell), queriedColumns);
      }
    }
    throw new IllegalStateException("Current page doesn't contain row: " + rowId);
  }

  private List<String> scanInterior(List<String> columns, Function<Row, Boolean> filter) {
    List<String> values = new ArrayList<>();
    Arrays.stream((InteriorTableCell[]) this.rootPage.getCells()).forEach(cell -> {
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
      values.addAll(this.scanLeaf(columns, filter));
    });
    DatabaseProducer.get().navigateToPageOfElement(this.rootPage.getRightmostPointer(), this);
    values.addAll(this.scanLeaf(columns, filter));
    return values;
  }

  private List<String> scanLeaf(List<String> queriedColumns, Function<Row, Boolean> filter) {
    if (!(currentPage.getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    return Arrays.stream(leafCells)
        .map(this::toRow)
        .filter(filter::apply)
        .map(row -> formatRowColumns(row, queriedColumns))
        .toList();
  }

  private Row toRow(LeafTableCell cell) {
    Map<String, Object> rowValue = new HashMap<>();
    for (int i = 0; i < this.columns.length; i++) {
      Column column = this.columns[i];
      // Replace [id] with [rowId] if [id] is not present
      if (column.name().equals("id")
          && column.type() == ColumnType.INTEGER
          && cell.getRecordBody().values()[i].length == 0) {
        rowValue.put(column.name(), Integer.toString(cell.getRowId()));
        continue;
      }
      rowValue.put(column.name(), retrieveValueOfColumn(i, cell.getRecordBody().values()[i]));
    }
    return new Row(rowValue);
  }

  private String formatRowColumns(Row row, List<String> columns) {
    return columns
        .stream()
        .map(col -> (String) row.get(col))
        .collect(Collectors.joining("|"));
  }


  private String retrieveValueOfColumn(int col, byte[] value) {
    return switch (this.columns[col].type()) {
      case TEXT -> new String(value);
      case INTEGER, REAL -> Double.toString(ByteUtils.toNumber(value).doubleValue());
      case NULL -> "null";
    };
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

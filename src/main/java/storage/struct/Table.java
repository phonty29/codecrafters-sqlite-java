package storage.struct;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import processing.Row;
import processing.planner.QueryPlanner;
import processing.planner.ScanType;
import processing.query.QueryEngine;
import processing.query.parser.ast.Column;
import processing.query.parser.ast.ColumnType;
import processing.query.parser.ast.Expression;
import processing.query.parser.ast.SelectStmt;
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

  public List<String> getByColumns(List<String> queriedColumns, SelectStmt select, Expression where) {
    List<String> orderedColumns = this.queryEngine.getColumnNames();
    int[] columnOrders = new int[queriedColumns.size()];
    int colIdx = 0;
    for (String queriedColumn : queriedColumns) {
      int idx = orderedColumns.indexOf(queriedColumn);
      if (idx != -1) {
        columnOrders[colIdx++] = idx;
      }
    }

    var planner = new QueryPlanner(select);
    if (planner.scanType().equals(ScanType.INDEX_SCAN)) {
      List<Integer> rowIds = planner.indexScanners().stream().map(IndexScanner::scan)
          .flatMap(List::stream).toList();
      return this.scanWithIndex(columnOrders, rowIds);
    }

    return switch (this.rootPage.getPageHeader().pageType()) {
      case LEAF_TABLE -> getByColumnsFromLeafTable(queriedColumns, where);
      case INT_TABLE -> getByColumnsFromInteriorTable(queriedColumns, where);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  public List<String> scanWithIndex(int[] columns, List<Integer> rowIds) {
    List<String> values = new ArrayList<>();
    BTreePage rootPage = this.currentPage;
    rowIds.forEach(rowId -> {
      this.currentPage = rootPage;
      values.add(this.getByRowId(columns, rowId));
    });
    return values;
  }

  public String getByRowId(int[] columns, Integer rowId) {
    return switch (this.currentPage.getPageHeader().pageType()) {
      case INT_TABLE -> getByRowIdFromInteriorTable(columns, rowId);
      case LEAF_TABLE -> getByRowIdFromLeafTable(columns, rowId);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  private String getByRowIdFromInteriorTable(int[] columns, Integer rowId) {
    if (!(currentPage.getCells() instanceof InteriorTableCell[] interiorTableCells)) {
      throw new IllegalStateException("Current page is not an interior table");
    }

    BTreePage parentPage = this.currentPage;
    for (var cell : interiorTableCells) {
      if (rowId <= cell.getRowId()) {
        DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
        return this.getByRowId(columns, rowId);
      }
    }
    DatabaseProducer.get().navigateToPageOfElement(parentPage.getRightmostPointer(), this);
    return this.getByRowId(columns, rowId);
  }

  private String getByRowIdFromLeafTable(int[] columns, Integer rowId) {
    if (!(currentPage.getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    for (var cell : leafCells) {
      if (rowId == cell.getRowId()) {
        return formatRowColumns(cell, columns);
      }
    }
    throw new IllegalStateException("Current page doesn't contain row: " + rowId);
  }

  private String formatRowColumns(LeafTableCell cell, int[] columns) {
    return Arrays.stream(columns)
        .mapToObj(col -> {
          Column column = this.columns[col];

          // Replace [id] with [rowId] if [id] is not present
          if (column.name().equals("id") && column.type() == ColumnType.INTEGER
              && cell.getRecordBody().values()[col].length == 0) {
            return Integer.toString(cell.getRowId());
          }

          return retrieveValueOfColumn(col, cell.getRecordBody().values()[col]);
        })
        .collect(Collectors.joining("|"));
  }

  private List<String> getByColumnsFromInteriorTable(List<String> columns, Expression where) {
    List<String> data = new ArrayList<>();
    Arrays.stream((InteriorTableCell[]) this.rootPage.getCells()).forEach(cell -> {
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
      data.addAll(this.getByColumnsFromLeafTable(columns, where));
    });
    DatabaseProducer.get().navigateToPageOfElement(this.rootPage.getRightmostPointer(), this);
    data.addAll(this.getByColumnsFromLeafTable(columns, where));
    return data;
  }

  private List<String> getByColumnsFromLeafTable(List<String> queriedColumns, Expression where) {
    if (!(currentPage.getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    return Arrays.stream(leafCells)
        .map(this::toRow)
        .filter(where::eval)
        .map(row -> formatRowColumns(row, queriedColumns))
        .toList();
  }

  private Row toRow(LeafTableCell cell) {
    Map<String, Object> rowValue = new HashMap<>();
    for (int i = 0; i < this.columns.length; i++) {
      Column column = this.columns[i];
      // Replace [id] with [rowId] if [id] is not present
      if (column.name().equals("id") && column.type() == ColumnType.INTEGER
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
      case INTEGER -> Long.toString(ByteUtils.toInteger(value).longValue());
      case REAL -> Double.toString(ByteUtils.toReal(value).doubleValue());
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

package struct.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import query_processor.SqlProcessor;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.ColumnType;
import struct.btree.BTreePage;
import struct.btree.BTreePageType;
import struct.cells.InteriorTableCell;
import struct.cells.LeafTableCell;
import struct.db.DatabaseProducer;
import struct.schema.Index.Row;
import utils.ByteUtils;

public class Table implements SchemaElement {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final Column[] columns;
  private BTreePage rootPage;
  private BTreePage currentPage;
  private Index index;

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
    this.sqlProcessor = new SqlProcessor(this.meta.sqlStmt);
    this.columns = sqlProcessor.getColumns();
  }

  public void setIndex(Index index) {
    validateIndex(index);
    this.index = index;
  }

  private void validateIndex(Index index) {
    if (Objects.isNull(index) || !index.meta().tableName().contentEquals(this.meta.name)) {
      throw new IllegalArgumentException("Invalid index for this table: " + index.meta().name());
    }
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

  public List<String> getByColumns(List<String> queriedColumns,
      Map<String, List<Function<String, Boolean>>> filters, String searchValue) {
    List<String> orderedColumns = this.sqlProcessor.getColumnNames();
    int[] columnOrders = new int[queriedColumns.size()];
    int colIdx = 0;
    for (String queriedColumn : queriedColumns) {
      int idx = orderedColumns.indexOf(queriedColumn);
      if (idx != -1) {
        columnOrders[colIdx++] = idx;
      }
    }

    boolean useIndex = Objects.nonNull(this.index)
        && filters.keySet().stream().anyMatch(col -> this.index.getColumn().contentEquals(col));
    if (useIndex) {
      DatabaseProducer.get().navigateTo(index);
      this.index.setSearchValue(searchValue);
      return this.getByIndexes(columnOrders, this.index.get().stream().map(Row::rowId).toList());
    }

    return switch (this.rootPage.getPageHeader().pageType()) {
      case LEAF_TABLE -> getByColumnsFromLeafTable(columnOrders, filters);
      case INT_TABLE -> getByColumnsFromInteriorTable(columnOrders, filters);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  public List<String> getByIndexes(int[] columns, List<Integer> rowIds) {
    List<String> values = new ArrayList<>();
    BTreePage rootPage = this.currentPage;
    rowIds.forEach(rowId -> {
      this.currentPage = rootPage;
      values.add(this.getByIndex(columns, rowId));
    });
    return values;
  }

  public String getByIndex(int[] columns, Integer rowId) {
    return switch (this.currentPage.getPageHeader().pageType()) {
      case INT_TABLE -> getByIndexFromInteriorTable(columns, rowId);
      case LEAF_TABLE -> getByIndexFromLeafTable(columns, rowId);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  private String getByIndexFromInteriorTable(int[] columns, Integer rowId) {
    if (!(currentPage.getCells() instanceof InteriorTableCell[] interiorTableCells)) {
      throw new IllegalStateException("Current page is not an interior table");
    }

    for (var cell : interiorTableCells) {
      if (rowId <= cell.getRowId()) {
        DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
        return this.getByIndex(columns, rowId);
      }
    }
    DatabaseProducer.get().navigateToPageOfElement(this.rootPage.getRightmostPointer(), this);
    return this.getByIndex(columns, rowId);
  }

  private String getByIndexFromLeafTable(int[] columns, Integer rowId) {
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

  /**
   * Returns values from table interior page (full-scan)
   *
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from all leaf table pages
   */
  private List<String> getByColumnsFromInteriorTable(int[] columns,
      Map<String, List<Function<String, Boolean>>> filterMap) {
    List<String> data = new ArrayList<>();
    Arrays.stream((InteriorTableCell[]) this.rootPage.getCells()).forEach(cell -> {
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this);
      data.addAll(this.getByColumnsFromLeafTable(columns, filterMap));
    });
    DatabaseProducer.get().navigateToPageOfElement(this.rootPage.getRightmostPointer(), this);
    data.addAll(this.getByColumnsFromLeafTable(columns, filterMap));
    return data;
  }

  /**
   * Returns values from table leaf page
   *
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from leaf table cells
   */
  private List<String> getByColumnsFromLeafTable(
      int[] columns,
      Map<String, List<Function<String, Boolean>>> filterMap
  ) {
    if (!(currentPage.getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    return Arrays.stream(leafCells)
        .filter(cell -> matchesFilters(cell, filterMap))
        .map(cell -> formatRowColumns(cell, columns))
        .toList();
  }

  private boolean matchesFilters(
      LeafTableCell cell,
      Map<String, List<Function<String, Boolean>>> filterMap
  ) {
    for (int i = 0; i < this.columns.length; i++) {
      String columnName = this.columns[i].name();
      List<Function<String, Boolean>> filters = filterMap.get(columnName);

      if (filters == null) {
        continue;
      }

      String value = retrieveValueOfColumn(i, cell.getRecordBody().values()[i]);
      if (!filters.stream().allMatch(f -> f.apply(value))) {
        return false;
      }
    }
    return true;
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

package struct;

import java.nio.ByteBuffer;
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
import struct.cells.InteriorTableCell;
import struct.cells.LeafTableCell;
import struct.db.DatabaseProducer;
import utils.ByteUtils;

public class Table {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
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
    this.sqlProcessor = new SqlProcessor(this.meta.sqlStmt);
    this.columns = sqlProcessor.getColumns();
  }

  public void setCurrentPage(BTreePage currentPage) {
    this.currentPage = currentPage;
  }

  public void setRootPage(BTreePage rootPage) {
    // Root page can be set only once
    if (Objects.isNull(this.rootPage)) {
      this.rootPage = rootPage;
      this.currentPage = rootPage;
    } else {
      throw new IllegalArgumentException("Cannot change root page");
    }
  }

  public int getRows() {
    if (Objects.nonNull(this.rootPage) && this.rootPage.getPageHeader().pageType()
        .equals(BTreePageType.LEAF_TABLE)) {
      return this.rootPage.getPageHeader().cellsCount();
    }
    throw new IllegalStateException("Root page is not a leaf table");
  }

  public List<String> getByColumns(List<String> queriedColumns,
      Map<String, List<Function<String, Boolean>>> filters) {
    List<String> orderedColumns = this.sqlProcessor.getColumnNames();
    int[] columnOrders = new int[queriedColumns.size()];
    int colIdx = 0;
    for (String queriedColumn : queriedColumns) {
      int idx = orderedColumns.indexOf(queriedColumn);
      if (idx != -1) {
        columnOrders[colIdx++] = idx;
      }
    }

    return switch (this.currentPage.getPageHeader().pageType()) {
      case LEAF_TABLE -> getByColumnsFromLeafTable(columnOrders, filters);
      case INT_TABLE -> getByColumnsFromInteriorTable(columnOrders, filters);
      default -> throw new IllegalStateException(
          "Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
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
      DatabaseProducer.get().navigateToPageOfTable(cell.getRootPage(), this);
      data.addAll(this.getByColumnsFromLeafTable(columns, filterMap));
    });
    DatabaseProducer.get().navigateToPageOfTable(this.rootPage.getRightmostPointer(), this);
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

  private int getRootPage(byte[] rootPageBytes) {
    return switch (rootPageBytes.length) {
      case 1 -> Byte.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).get());
      case 2 -> Short.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).getShort());
      case 4 -> ByteBuffer.wrap(rootPageBytes).getInt();
      default -> throw new IllegalStateException("Rootpage couldn't be cast to integer type");
    };
  }

  public record Meta(
      String name,
      int rootPageNumber,
      String sqlStmt
  ) {

  }
}

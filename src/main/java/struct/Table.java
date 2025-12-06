package struct;

import java.io.IOException;
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
import utils.ByteUtils;

public class Table {

  private final Database database; // make global acceptable and singleton
  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final Column[] columns;
  private BTreePage rootPage;
  private BTreePage currentPage;

  public Table(Database db, LeafTableCell leafTableCell) {
    this.database = db;
    // Get meta from sqlite_schema cells
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int sqlStmtOrder = 4;
    byte[][] cellValues = leafTableCell.getRecordBody().values();
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

  public BTreePage getRootPage() {
    return this.rootPage;
  }

  public int getRows() throws IOException {
    if (Objects.nonNull(this.rootPage) && this.rootPage.getPageHeader().pageType().equals(BTreePageType.LEAF_TABLE)) {
      return this.rootPage.getPageHeader().cellsCount();
    }
    throw new IllegalStateException("Root page is not a leaf table");
  }

  public List<String> getByColumns(List<String> queriedColumns, Map<String, List<Function<String, Boolean>>> filters)
      throws IOException {
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
      case LEAF_TABLE -> getByColumns(columnOrders, filters);
      case INT_TABLE -> getByColumnsForInteriorTable(columnOrders, filters);
      default -> throw new IllegalStateException("Not supported page type: " + this.currentPage.getPageHeader().pageType());
    };
  }

  private List<String> getByColumnsForInteriorTable(int[] columns,
      Map<String, List<Function<String, Boolean>>> filterMap) throws IOException {
    List<String> data = new ArrayList<>();
    int rightmostPointer = this.currentPage.getRightmostPointer();
    for (var cell : (InteriorTableCell[]) this.currentPage.getCells()) {
      this.database.setPageOfTable(cell.getRootPage(), this);
      data.addAll(this.getByColumns(columns, filterMap));
    }
    this.database.setPageOfTable(rightmostPointer, this);
    data.addAll(this.getByColumns(columns, filterMap));
    return data;
  }

  /**
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from cells
   */
  private List<String> getByColumns(int[] columns,
      Map<String, List<Function<String, Boolean>>> filterMap) {
    if (!(this.currentPage.getCells() instanceof LeafTableCell[])) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    return Arrays.stream((LeafTableCell[]) this.currentPage.getCells())
        .filter(cell -> {
          boolean condition = true;
          for (int i = 0; i < this.columns.length; i++) {
            String filterKey = this.columns[i].name();
            List<Function<String, Boolean>> filters = filterMap.get(filterKey);
            if (Objects.isNull(filters)) {
              continue;
            }
            String value = getValueOfColumn(i, cell.getRecordBody().values()[i]);
            condition = filters.stream().allMatch(filter -> filter.apply(value));
          }
          return condition;
        })
        .map(cell ->
            Arrays.stream(columns)
                .mapToObj(col -> {
                  if (this.columns[col].name().contentEquals("id") && this.columns[col].type().equals(
                      ColumnType.INTEGER)) {
                    return Integer.toString(cell.getRowId());
                  }
                  return getValueOfColumn(col, cell.getRecordBody().values()[col]);
                })
                .collect(Collectors.joining("|"))
        )
        .toList();
  }

  private String getValueOfColumn(int col, byte[] value) {
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

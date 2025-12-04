package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import query_processor.SqlProcessor;
import query_processor.parser.ast.Column;
import utils.ByteUtils;

public class Table {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final Column[] columns;
  private BTreePage tablePage;


  public Table(LeafTableCell leafTableCell) {
    // Get meta from sqlite_schema cells
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int sqlStmtOrder = 4;
    byte[][] cellValues = leafTableCell.getRecordBody().values();
    String tableName = new String(cellValues[tableNameOrder]);
    int rootPage = getRootPage(cellValues[rootPageOrder]);
    String sqlStmt = new String(cellValues[sqlStmtOrder]);
    this.meta = new Meta(tableName, rootPage, sqlStmt);

    // Init table structure
    this.sqlProcessor = new SqlProcessor(this.meta.sqlStmt);
    this.columns = sqlProcessor.getColumns();
  }

  public void setTablePage(BTreePage tablePage) {
    this.tablePage = tablePage;
  }

  public int getRows() throws IOException {
    return this.tablePage.getPageHeader().cellsCount();
  }

  public List<String> getByColumns(List<String> queriedColumns, Map<String, List<Function<String, Boolean>>> filters) {
    List<String> orderedColumns = this.sqlProcessor.getColumnNames();
    int[] columnOrders = new int[queriedColumns.size()];
    int colIdx = 0;
    for (String queriedColumn : queriedColumns) {
      int idx = orderedColumns.indexOf(queriedColumn);
      if (idx != -1) {
        columnOrders[colIdx++] = idx;
      }
    }
    return this.getByColumns(columnOrders, filters);
  }

  /**
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from cells (not typed!)
   */
  private List<String> getByColumns(int[] columns,
      Map<String, List<Function<String, Boolean>>> filterMap) {
    return Arrays.stream(this.tablePage.getCells())
        .map(LeafTableCell::getRecordBody)
        .filter(recordBody -> {
          boolean condition = true;
          for (int i = 0; i < this.columns.length; i++) {
            String filterKey = this.columns[i].name();
            List<Function<String, Boolean>> filters = filterMap.get(filterKey);
            if (Objects.isNull(filters)) {
              continue;
            }
            String value = getValueOfColumn(i, recordBody.values()[i]);
            condition = filters.stream().allMatch(filter -> filter.apply(value));
          }
          return condition;
        })
        .map(recordBody ->
            Arrays.stream(columns)
                .mapToObj(col -> getValueOfColumn(col, recordBody.values()[col]))
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
      int rootPage,
      String sqlStmt
  ) {

  }
}

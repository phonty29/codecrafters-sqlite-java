package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import query_processor.SqlProcessor;
import query_processor.parser.ast.Column;
import utils.ByteUtils;

public class Table {

  private final Meta meta;
  private final SqlProcessor sqlProcessor;
  private final Column[] columns;
  private BTreePage tablePage;


  public Table(Cell cell) {
    // Get meta from sqlite_schema cells
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int sqlStmtOrder = 4;
    byte[][] cellValues = cell.getRecordBody().values();
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

  public List<String> getByColumns(List<String> queriedColumns) {
    List<String> orderedColumns = this.sqlProcessor.getColumnNames();
    int[] columnOrders = new int[queriedColumns.size()];
    int colIdx = 0;
    for (String queriedColumn : queriedColumns) {
      int idx = orderedColumns.indexOf(queriedColumn);
      if (idx != -1) {
        columnOrders[colIdx++] = idx;
      }
    }
    return this.getByColumns(columnOrders);
  }

  /**
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from cells (not typed!)
   */
  private List<String> getByColumns(int[] columns) {
    return Arrays.stream(this.tablePage.getCells())
        .map(Cell::getRecordBody)
        .map(recordBody ->
            Arrays.stream(columns)
                .mapToObj(col -> switch (this.columns[col].type()) {
                  case TEXT -> new String(recordBody.values()[col]);
                  case INTEGER ->
                      Long.toString(ByteUtils.toInteger(recordBody.values()[col]).longValue());
                  case REAL ->
                      Double.toString(ByteUtils.toReal(recordBody.values()[col]).doubleValue());
                  case NULL -> "null";
                })
                .collect(Collectors.joining("|"))
        )
        .toList();
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

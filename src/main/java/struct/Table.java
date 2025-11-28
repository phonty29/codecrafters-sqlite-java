package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import query_processor.SqlProcessor;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.CreateStmt;

public class Table {

  private final Meta meta;
  private BTreePage tablePage;
  private final Column[] columns;


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
    SqlProcessor sqlProcessor = new SqlProcessor();
    this.columns =
        ((CreateStmt) sqlProcessor.process(this.meta().sqlStmt()))
            .columns()
            .toArray(Column[]::new);
  }

  public void setTablePage(BTreePage tablePage) {
    this.tablePage = tablePage;
  }

  public int getRows() throws IOException {
    return this.tablePage.getPageHeader().cellsCount();
  }

  public List<String> getByColumns(List<String> queriedColumns) throws IOException {
    List<String> orderedColumns =
        ((CreateStmt) new SqlProcessor().process(
            this.meta().sqlStmt()))
            .columns()
            .stream()
            .map(Column::name)
            .toList();
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
   * REFACTOR! Proper retrieve for types INTEGER, REAL. Now it doesn't count the size
   *
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from cells (not typed!)
   * @throws IOException
   */
  private List<String> getByColumns(int[] columns) throws IOException {
    return Arrays.stream(this.tablePage.getCells())
        .map(Cell::getRecordBody)
        .map(recordBody ->
            Arrays.stream(columns)
                .mapToObj(col -> switch (this.columns[col].type()) {
                  case TEXT -> new String(recordBody.values()[col]);
                  case INTEGER -> Integer.toString(ByteBuffer.wrap(recordBody.values()[col]).getInt());
                  default -> "null";
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

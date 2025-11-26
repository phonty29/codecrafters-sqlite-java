package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Table {

  private final Meta meta;
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
  }

  public void setTablePage(BTreePage tablePage) {
    this.tablePage = tablePage;
  }

  public int getRows() throws IOException {
    return this.tablePage.getPageHeader().cellsCount();
  }

  /**
   * REFACTOR! Types depend on column type in this.sqlStmt
   *
   * @param columns - the order of columns in the table b-tree page structure
   * @return list of values from cells (not typed!)
   * @throws IOException
   */
  public List<String> getByColumns(int[] columns) throws IOException {
    return Arrays.stream(this.tablePage.getCells())
        .map(Cell::getRecordBody)
        .map(recordBody ->
            Arrays.stream(columns)
                .mapToObj(col -> new String(recordBody.values()[col]))
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

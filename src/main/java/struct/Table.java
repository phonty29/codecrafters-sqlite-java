package struct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class Table {

  // Meta for Table
  private final String tableName;
  private final int rootPage;
  private final String sqlStmt;

  private BTreePage tablePage;


  public Table(Cell cell) {
    // Get meta from sqlite_schema cells
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int sqlStmtOrder = 4;
    byte[][] cellValues = cell.getRecordBody().values();
    this.tableName = new String(cellValues[tableNameOrder]);
    this.rootPage = getRootPage(cellValues[rootPageOrder]);
    this.sqlStmt = new String(cellValues[sqlStmtOrder]);
  }

  public void setTablePage(BTreePage tablePage) {
    this.tablePage = tablePage;
  }

  public int getRows() throws IOException {
    return this.tablePage.getPageHeader().cellsCount();
  }

  // REFACTOR! Types depend on column type in this.sqlStmt
  public List<String> getByColumn(int column) throws IOException {
    return Arrays.stream(this.tablePage.getCells()).map(Cell::getRecordBody)
        .map(recordBody -> new String(recordBody.values()[column])).toList();
  }

  public String getTableName() {
    return this.tableName;
  }

  public int getRootPage() {
    return this.rootPage;
  }

  public String getSqlStmt() {
    return this.sqlStmt;
  }

  private int getRootPage(byte[] rootPageBytes) {
    return switch (rootPageBytes.length) {
      case 1 -> Byte.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).get());
      case 2 -> Short.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).getShort());
      case 4 -> ByteBuffer.wrap(rootPageBytes).getInt();
      default -> throw new IllegalStateException("Rootpage couldn't be cast to integer type");
    };
  }
}

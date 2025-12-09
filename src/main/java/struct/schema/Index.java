package struct.schema;

import struct.cells.LeafTableCell;

public class Index implements SchemaElement {

  private final Meta meta;

  public Index(LeafTableCell cell) {
    // Get meta from sqlite_schema cells
    int indexNameOrder = 1;
    int tableNameOrder = 2;
    int rootPageOrder = 3;
    int createStmtOrder = 4;
    byte[][] cellValues = cell.getRecordBody().values();
    String indexName = new String(cellValues[indexNameOrder]);
    String tableName = new String(cellValues[tableNameOrder]);
    int rootPageNumber = getRootPage(cellValues[rootPageOrder]);
    String sqlStmt = new String(cellValues[createStmtOrder]);
    this.meta = new Meta(indexName, tableName, rootPageNumber, sqlStmt);

  }

  public Meta getMeta() {
    return this.meta;
  }

  public record Meta(
      String name,
      String tableName,
      int rootPage,
      String createStmt
  ) {

  }
}
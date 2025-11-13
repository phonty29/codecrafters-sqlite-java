package executors;

import db.Database;
import db.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class SqlSimpleExecutor implements Executor {

  private final String sqlCommand;
  private final String tableName;

  public SqlSimpleExecutor(String sqlCommand) {
    this.sqlCommand = sqlCommand;
    String[] splitSqlCommand = this.sqlCommand.trim().split("\\s+");
    this.tableName = splitSqlCommand[splitSqlCommand.length - 1];
  }

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      Database database = new Database(databaseFile);
      // Skip till cell pointer array
      ByteBuffer pageBuffer = database.getCurrentPageBuffer();

      // Set position to after the "number of cells on the page"
      pageBuffer.position(105);

      int cellHeaderTailLength = 3;
      pageBuffer.position(pageBuffer.position() + cellHeaderTailLength);
      int numberOfTables = database.getNumberOfTables();

      // Slice Cell Pointer Array as buffer
      ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(),
          2 * numberOfTables);

      short[] offsets = new short[numberOfTables];
      Table table = null;
      for (int i = 0; i < numberOfTables; i++) {
        // The last table offset goes first, so printing order from last-to-first
        offsets[i] = cellPointerArrayBuffer.getShort();
        if (i == 0) {
          table = new Table(pageBuffer.position(offsets[i]).slice());
        } else {
          table = new Table(pageBuffer.slice(offsets[i], offsets[i - 1] - offsets[i]));
        }
        if (table.getTableName().contentEquals(this.tableName)) {
          break;
        }
      }

      if (Objects.nonNull(table)) {
        database.setCurrentPage(table.getRootPage());
        pageBuffer = database.getCurrentPageBuffer();
        table.setTablePageBuffer(pageBuffer);
        System.out.println(table.getRows());
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

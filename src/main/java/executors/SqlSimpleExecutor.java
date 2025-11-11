package executors;

import db.DbFile;
import db.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
      DbFile dbFile = new DbFile(databaseFile);
      // Skip till cell pointer array
      ByteBuffer pageBuffer = dbFile.getPageBuffer();
      // Set position to after the "number of cells on the page"
      pageBuffer.position(105);

      int cellHeaderTailLength = 3;
      pageBuffer.position(pageBuffer.position() + cellHeaderTailLength);
      int numberOfTables = dbFile.getNumberOfTables();

      // Slice Cell Pointer Array as buffer
      ByteBuffer cellPointerArrayBuffer = pageBuffer.slice(pageBuffer.position(),
          2 * numberOfTables);

      short[] offsets = new short[numberOfTables];
      Optional<Table> table = Optional.empty();
      for (int i = 0; i < numberOfTables; i++) {
        // The last table offset goes first, so printing order from last-to-first
        offsets[i] = cellPointerArrayBuffer.getShort();
        if (i == 0) {
          table = Optional.of(new Table(pageBuffer.position(offsets[i]).slice()));
        } else {
          table = Optional.of(new Table(pageBuffer.slice(offsets[i], offsets[i - 1] - offsets[i])));
        }
        if (table.get().getTableName().contentEquals(this.tableName)) {
          break;
        }
      }

      if (table.isPresent()) {
        dbFile.setPage(table.get().getRootPage());
      }
      pageBuffer = dbFile.getPageBuffer();

      int rows = Short.toUnsignedInt(pageBuffer.position(3).getShort());
      System.out.println(rows);

    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

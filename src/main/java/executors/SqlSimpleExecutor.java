package executors;

import db.Database;
import db.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
      Optional<Table> optionalTable = Arrays.stream(database.getTables())
          .filter(t -> t.getTableName().contains(tableName))
          .findFirst();
      if (optionalTable.isPresent()) {
        Table table = optionalTable.get();
        database.setCurrentPage(table.getRootPage());
        ByteBuffer pageBuffer = database.getCurrentPageBuffer();
        table.setTablePageBuffer(pageBuffer);
        System.out.println(table.getRows());
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

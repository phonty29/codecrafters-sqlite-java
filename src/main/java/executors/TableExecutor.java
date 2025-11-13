package executors;

import db.Database;
import db.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class TableExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      Database database = new Database(databaseFile);
      System.out.println(String.join(" ", Arrays.stream(database.getTables()).map(
          Table::getTableName).toList()));
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

package executors;

import db.Database;
import java.io.FileInputStream;
import java.io.IOException;

public class DbInfoExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      Database database = new Database(databaseFile);
      // Print results
      System.out.println("database page size: " + database.getPageSize());
      System.out.println("number of tables: " + database.getNumberOfTables());
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

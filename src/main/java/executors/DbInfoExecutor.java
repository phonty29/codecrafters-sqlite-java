package executors;

import db.DbFile;
import java.io.FileInputStream;
import java.io.IOException;

public class DbInfoExecutor implements Executor {

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      DbFile dbFile = new DbFile(databaseFile);
      // Print results
      System.out.println("database page size: " + dbFile.getPageSize());
      System.out.println("number of tables: " + dbFile.getNumberOfTables());
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

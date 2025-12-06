package executors;

import java.io.FileInputStream;
import java.io.IOException;
import struct.db.Database;
import struct.db.DatabaseProducer;

public class DbInfoExecutor implements Executor {

  @Override
  public void execute() {
    var database = DatabaseProducer.get();
    // Print results
    System.out.println("database page size: " + database.getPageSize());
    System.out.println("number of tables: " + database.getNumberOfTables());
  }
}

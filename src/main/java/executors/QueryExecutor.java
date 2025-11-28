package executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import query_processor.SqlProcessor;
import struct.Database;

public class QueryExecutor implements Executor {

  private final SqlProcessor queryProcessor;

  public QueryExecutor(String query) {
    this.queryProcessor = new SqlProcessor(query);
  }

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      Database database = new Database(databaseFile);

      String tableName = queryProcessor.tableName();
      var table = database.getTable(tableName);
      database.navigateToTable(table);

      // select count(*) from table;
      if (this.queryProcessor.isCount()) {
        System.out.println(table.getRows());
      }

      // select [columnName, ...] from [tableName];
      if (queryProcessor.isColumnsRetrieval()) {
        List<String> queriedColumns = this.queryProcessor.getColumnNames();
        table.getByColumns(queriedColumns).forEach(System.out::println);
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

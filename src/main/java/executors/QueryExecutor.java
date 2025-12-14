package executors;

import java.util.List;
import query_processor.SqlProcessor;
import struct.db.DatabaseProducer;

public class QueryExecutor implements Executor {

  private final SqlProcessor queryProcessor;

  public QueryExecutor(String query) {
    this.queryProcessor = new SqlProcessor(query);
  }

  @Override
  public void execute() {
    var database = DatabaseProducer.get();

    String tableName = queryProcessor.tableName();
    var table = database.getTable(tableName);
    database.navigateTo(table);

    // select count(*) from table;
    if (this.queryProcessor.isCount()) {
      System.out.println(table.getCellsCount());
    }

    // select [columnName, ...] from [tableName];
    if (queryProcessor.isColumnsRetrieval()) {
      List<String> queriedColumns = this.queryProcessor.getColumnNames();
      table
          .getByColumns(queriedColumns, this.queryProcessor.filters(), this.queryProcessor.getSearchedValue())
          .forEach(System.out::println);
    }
  }
}

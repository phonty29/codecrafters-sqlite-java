package executors;

import java.util.List;
import processing.planner.QueryPlanner;
import processing.query.QueryEngine;
import storage.db.DatabaseProducer;

public class QueryExecutor implements Executor {

  private final QueryEngine queryProcessor;

  public QueryExecutor(String query) {
    this.queryProcessor = new QueryEngine(query);
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
          .getByColumns(queriedColumns, this.queryProcessor.select(), this.queryProcessor.where())
          .forEach(System.out::println);
    }
  }
}

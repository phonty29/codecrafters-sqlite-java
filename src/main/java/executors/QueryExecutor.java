package executors;

import java.util.List;
import qprocessor.compiler.QueryCompiler;
import qprocessor.compiler.parser.ast.Identifier;
import qprocessor.compiler.parser.ast.SelectItem;
import qprocessor.compiler.parser.ast.TableRef;
import qprocessor.planner.QueryPlanner;
import storage.db.DatabaseProducer;

public class QueryExecutor implements Executor {

  private final QueryCompiler compiler;

  public QueryExecutor(String query) {
    this.compiler = new QueryCompiler(query);
  }

  @Override
  public void execute() {
    var database = DatabaseProducer.get();

    String tableName = ((TableRef) compiler.select().from()).name();
    var table = database.getTable(tableName);
    database.navigateTo(table);

    // select count(*) from table;
    if (this.compiler.isCount()) {
      System.out.println(table.getCellsCount());
      return;
    }

    // select [columnName, ...] from [tableName];
    List<SelectItem> sItems = compiler.select().list();
    if (
        !sItems.isEmpty()
            && sItems.stream().allMatch(it -> it.expr() instanceof Identifier)
    ) {
      List<String> queriedColumns = this.compiler
          .select()
          .list()
          .stream()
          .map(i -> ((Identifier) i.expr()).name())
          .toList();

      table
          .scanner()
          .scan(queriedColumns, new QueryPlanner(this.compiler.select()))
          .forEach(System.out::println);
    }
  }
}

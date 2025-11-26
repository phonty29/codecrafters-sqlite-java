package executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import query_processor.SqlProcessor;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.CreateStmt;
import query_processor.parser.ast.FunctionCall;
import query_processor.parser.ast.Identifier;
import query_processor.parser.ast.SelectItem;
import query_processor.parser.ast.SelectStmt;
import query_processor.parser.ast.TableRef;
import struct.Database;
import struct.Table;

public class QueryExecutor implements Executor {

  private final SqlProcessor sqlProcessor;
  private final SelectStmt queryTree;

  public QueryExecutor(String query) {
    this.sqlProcessor = new SqlProcessor();
    this.queryTree = (SelectStmt) this.sqlProcessor.process(query);
  }

  @Override
  public void execute(String filePath) {
    try (FileInputStream databaseFile = new FileInputStream(filePath)) {
      Database database = new Database(databaseFile);

      String tableName = ((TableRef) this.queryTree.from()).name();
      Table table = Arrays.stream(database.getTables())
          .filter(t -> t.meta().name().contains(tableName))
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("Required table not found: " + tableName));

      database.navigateToTable(table);

      List<SelectItem> items = this.queryTree.selectList();
      // select count(*) from table;
      if (items.size() == 1 &&
          items.getFirst().expr() instanceof FunctionCall &&
          ((FunctionCall) items.getFirst().expr()).name().equals("count")
      ) {
        System.out.println(table.getRows());
      }

      // select [columnName, ...] from [tableName];
      if (!items.isEmpty() && items.getFirst().expr() instanceof Identifier) {
        List<String> queriedColumns = items
            .stream()
            .map(i -> ((Identifier) i.expr()).name())
            .toList();

        List<String> orderedColumns = ((CreateStmt) this.sqlProcessor.process(table.meta().sqlStmt()))
            .columns()
            .stream()
            .map(Column::name)
            .toList();

        int[] columnOrders = new int[queriedColumns.size()];
        int colIdx = 0;
        for (String queriedColumn : queriedColumns) {
          int idx = orderedColumns.indexOf(queriedColumn);
          if (idx != -1) {
            columnOrders[colIdx++] = idx;
          }
        }
        table.getByColumns(columnOrders).forEach(System.out::println);
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

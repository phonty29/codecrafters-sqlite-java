package executors;

import struct.Database;
import struct.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import query_processor.SqlProcessor;
import query_processor.parser.ast.CreateStmt;
import query_processor.parser.ast.FunctionCall;
import query_processor.parser.ast.Identifier;
import query_processor.parser.ast.SelectItem;
import query_processor.parser.ast.SelectStmt;
import query_processor.parser.ast.TableRef;

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
          .filter(t -> t.getTableName().contains(tableName))
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

      // select [columnName] from [tableName];
      if (items.size() == 1 &&
          items.getFirst().expr() instanceof Identifier
      ) {
        String column = ((Identifier) items.getFirst().expr()).name();
        CreateStmt createStmt = (CreateStmt) this.sqlProcessor.process(table.getSqlStmt());
        for (int i = 0; i < createStmt.columns().size(); i++) {
          if (column.contentEquals(createStmt.columns().get(i).name())) {
            table.getByColumn(i).forEach(System.out::println);
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

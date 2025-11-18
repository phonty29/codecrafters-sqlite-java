package executors;

import db.Database;
import db.Table;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import processor.lexer.SqlLexer;
import processor.lexer.Token;
import processor.parser.SqlParser;
import processor.parser.ast.FunctionCall;
import processor.parser.ast.Identifier;
import processor.parser.ast.SelectItem;
import processor.parser.ast.SelectStmt;
import processor.parser.ast.TableRef;

public class QueryExecutor implements Executor {
  private final SelectStmt queryTree;

  public QueryExecutor(String query) {
    List<Token> tokens = new SqlLexer(query).tokenize();
    this.queryTree = new SqlParser(tokens).parseSelect();
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

      database.setCurrentPage(table.getRootPage());
      ByteBuffer pageBuffer = database.getCurrentPageBuffer();
      table.setTablePageBuffer(pageBuffer);

      List<SelectItem> items = this.queryTree.selectList();
      // select count(*) from table;
      if (items.size() == 1 &&
          items.getFirst().expr() instanceof FunctionCall &&
          ((FunctionCall) items.getFirst().expr()).name().equals("count")
      ) {
        System.out.println(table.getRows());
      }

      if (items.size() == 1 &&
          items.getFirst().expr() instanceof Identifier
      ) {
        String column = ((Identifier) items.getFirst().expr()).name();
        System.out.println("Required column: " + column);
        System.out.println("SQL statement: " + table.getSqlStmt());
      }

    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }
}

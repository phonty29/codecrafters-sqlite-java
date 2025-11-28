package query_processor;

import java.util.List;
import query_processor.lexer.SqlLexer;
import query_processor.lexer.Token;
import query_processor.parser.SqlParser;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.CreateStmt;
import query_processor.parser.ast.FunctionCall;
import query_processor.parser.ast.Identifier;
import query_processor.parser.ast.SelectItem;
import query_processor.parser.ast.SelectStmt;
import query_processor.parser.ast.Statement;
import query_processor.parser.ast.TableRef;

public class SqlProcessor {
  private final Statement queryTree;

  public SqlProcessor(String query) {
    SqlLexer lexer = new SqlLexer(query);
    List<Token> tokens = lexer.tokenize();
    SqlParser parser = new SqlParser(tokens);
    this.queryTree = switch (query) {
      case String q when q.toLowerCase().startsWith("select") -> parser.parseSelect();
      case String q when q.toLowerCase().startsWith("create") -> parser.parseCreateStmt();
      default -> throw new IllegalArgumentException("Unknown query: " + query);
    };
  }

  private SelectStmt select() {
    if (this.queryTree instanceof SelectStmt) {
      return (SelectStmt) this.queryTree;
    }
    throw new IllegalStateException("Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  private CreateStmt create() {
    if (this.queryTree instanceof CreateStmt) {
      return (CreateStmt) this.queryTree;
    }
    throw new IllegalStateException("Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  public String tableName() {
    return ((TableRef) select().from()).name();
  }

  public boolean isCount() {
    List<SelectItem> items = select().list();
    // select count(*) from table;
    return items.size() == 1 &&
        items.getFirst().expr() instanceof FunctionCall &&
        ((FunctionCall) items.getFirst().expr()).name().equals("count");
  }

  public boolean isColumnsRetrieval() {
    List<SelectItem> items = select().list();
    return !items.isEmpty()
        && items.stream().allMatch(it -> it.expr() instanceof Identifier);
  }

  public List<String> getColumnNames() {
    if (this.queryTree instanceof SelectStmt) {
      return select()
          .list()
          .stream()
          .map(i -> ((Identifier) i.expr()).name())
          .toList();
    } else if (this.queryTree instanceof CreateStmt) {
        return create()
            .columns()
            .stream()
            .map(Column::name)
            .toList();
    }
    else {
      throw new IllegalStateException("Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }

  public Column[] getColumns() {
    if (this.queryTree instanceof CreateStmt) {
      return create()
          .columns()
          .toArray(Column[]::new);
    }
    else {
      throw new IllegalStateException("Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }
}

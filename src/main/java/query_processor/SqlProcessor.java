package query_processor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import query_processor.lexer.SqlLexer;
import query_processor.lexer.Token;
import query_processor.parser.SqlParser;
import query_processor.parser.ast.BinaryOp;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.CreateIndexStmt;
import query_processor.parser.ast.CreateTableStmt;
import query_processor.parser.ast.FunctionCall;
import query_processor.parser.ast.Identifier;
import query_processor.parser.ast.Literal;
import query_processor.parser.ast.SelectItem;
import query_processor.parser.ast.SelectStmt;
import query_processor.parser.ast.Statement;
import query_processor.parser.ast.TableRef;

public class SqlProcessor {

  private final Statement queryTree;

  public SqlProcessor(String query) {
    query = query
        .replaceAll("\"", "")
        .toLowerCase()
        .trim();
    SqlLexer lexer = new SqlLexer(query);
    List<Token> tokens = lexer.tokenize();
    SqlParser parser = new SqlParser(tokens);
    this.queryTree = switch (query) {
      case String q when q.toLowerCase().startsWith("select") -> parser.parseSelect();
      case String q when q.toLowerCase().startsWith("create table") -> parser.parseCreateTable();
      case String q when q.toLowerCase().startsWith("create index") -> parser.parseCreateIndex();
      default -> throw new IllegalArgumentException("Unknown query: " + query);
    };
  }

  private SelectStmt select() {
    if (this.queryTree instanceof SelectStmt) {
      return (SelectStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  private CreateTableStmt create() {
    if (this.queryTree instanceof CreateTableStmt) {
      return (CreateTableStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  private CreateIndexStmt createIndex() {
    if (this.queryTree instanceof CreateIndexStmt) {
      return (CreateIndexStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown query type: " + this.queryTree.getClass().getSimpleName());
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
    } else if (this.queryTree instanceof CreateTableStmt) {
      return create()
          .columns()
          .stream()
          .map(Column::name)
          .toList();
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }

  public Map<String, List<Function<String, Boolean>>> filters() {
    if (this.queryTree instanceof SelectStmt && Objects.nonNull(select().where())) {
      var where = (BinaryOp) select().where();
      if (where.left() instanceof Identifier && where.right() instanceof Literal) {
        Function<String, Boolean> filter = (val) -> {
          if (where.op().contentEquals("=")) {
            var literal = (Literal) where.right();
            return literal.value().equals(val.toLowerCase());
          }
          throw new IllegalArgumentException("Unknown filter: " + val);
        };

        String filterKey = ((Identifier) where.left()).name();
        return Map.of(filterKey, List.of(filter));
      }
    }
    return Map.of();
  }

  public String getSearchedValue() {
    if (this.queryTree instanceof SelectStmt && Objects.nonNull(select().where())) {
      var where = (BinaryOp) select().where();
      if (where.left() instanceof Identifier && where.right() instanceof Literal) {
        if (where.op().contentEquals("=")) {
          var literal = (Literal) where.right();
          return (String) literal.value();
        }
      }
    }
    return "";
  }

  public Column[] getColumns() {
    if (this.queryTree instanceof CreateTableStmt) {
      return create()
          .columns()
          .toArray(Column[]::new);
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }

  public List<String> getIndexedColumns() {
    if (this.queryTree instanceof CreateIndexStmt) {
      return createIndex().columns();
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }
}

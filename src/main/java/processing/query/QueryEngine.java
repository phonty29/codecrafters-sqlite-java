package processing.query;

import java.util.List;
import java.util.Objects;
import processing.query.lexer.SqlLexer;
import processing.query.lexer.Token;
import processing.query.parser.SqlParser;
import processing.query.parser.ast.BinaryOp;
import processing.query.parser.ast.Column;
import processing.query.parser.ast.CreateIndexStmt;
import processing.query.parser.ast.CreateTableStmt;
import processing.query.parser.ast.Expression;
import processing.query.parser.ast.FunctionCall;
import processing.query.parser.ast.Identifier;
import processing.query.parser.ast.Literal;
import processing.query.parser.ast.SelectItem;
import processing.query.parser.ast.SelectStmt;
import processing.query.parser.ast.Statement;
import processing.query.parser.ast.TableRef;

public class QueryEngine {
  private final Statement queryTree;
  private final StmtType stmtType;

  public QueryEngine(String query) {
    query = prepareQuery(query);
    SqlLexer lexer = new SqlLexer(query);
    List<Token> tokens = lexer.tokenize();
    SqlParser parser = new SqlParser(tokens);
    this.queryTree = switch (query) {
      case String q when q.toLowerCase().startsWith("select") -> {
        this.stmtType = StmtType.SELECT;
        yield parser.parseSelect();
      }
      case String q when q.toLowerCase().startsWith("create table") -> {
        this.stmtType = StmtType.CREATE_TABLE;
        yield parser.parseCreateTable();
      }
      case String q when q.toLowerCase().startsWith("create index") -> {
        this.stmtType = StmtType.CREATE_INDEX;
        yield parser.parseCreateIndex();
      }
      default -> throw new IllegalArgumentException("Unknown query: " + query);
    };
  }

  private String prepareQuery(String query) {
    return query
        .replaceAll("\"", "")
        .toLowerCase()
        .trim();
  }

  public SelectStmt select() {
    if (this.stmtType.equals(StmtType.SELECT) && this.queryTree instanceof SelectStmt) {
      return (SelectStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  public CreateTableStmt createTable() {
    if (this.stmtType.equals(StmtType.CREATE_TABLE) && this.queryTree instanceof CreateTableStmt) {
      return (CreateTableStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown query type: " + this.queryTree.getClass().getSimpleName());
  }

  public CreateIndexStmt createIndex() {
    if (this.stmtType.equals(StmtType.CREATE_INDEX) && this.queryTree instanceof CreateIndexStmt) {
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
      return createTable()
          .columns()
          .stream()
          .map(Column::name)
          .toList();
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }

  public Expression where() {
    if (this.queryTree instanceof SelectStmt && Objects.nonNull(select().where())) {
      return select().where();
    }
    return (_) -> true;
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
      return createTable()
          .columns()
          .toArray(Column[]::new);
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }

  public String getIndexedColumn() {
    if (this.queryTree instanceof CreateIndexStmt) {
      return createIndex().column();
    } else {
      throw new IllegalStateException(
          "Unknown query type: " + this.queryTree.getClass().getSimpleName());
    }
  }
}

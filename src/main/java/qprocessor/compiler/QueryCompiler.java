package qprocessor.compiler;

import java.util.List;
import qprocessor.compiler.lexer.SqlLexer;
import qprocessor.compiler.lexer.Token;
import qprocessor.compiler.parser.SqlParser;
import qprocessor.compiler.parser.ast.Column;
import qprocessor.compiler.parser.ast.CreateIndexStmt;
import qprocessor.compiler.parser.ast.CreateTableStmt;
import qprocessor.compiler.parser.ast.FunctionCall;
import qprocessor.compiler.parser.ast.SelectItem;
import qprocessor.compiler.parser.ast.SelectStmt;
import qprocessor.compiler.parser.ast.Statement;

public class QueryCompiler {

  private final Statement queryTree;
  private final StmtType stmtType;

  public QueryCompiler(String query) {
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
      default -> throw new IllegalArgumentException("Unknown compiler: " + query);
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
        "Unknown compiler type: " + this.queryTree.getClass().getSimpleName());
  }

  public CreateTableStmt createTable() {
    if (this.stmtType.equals(StmtType.CREATE_TABLE) && this.queryTree instanceof CreateTableStmt) {
      return (CreateTableStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown compiler type: " + this.queryTree.getClass().getSimpleName());
  }

  public CreateIndexStmt createIndex() {
    if (this.stmtType.equals(StmtType.CREATE_INDEX) && this.queryTree instanceof CreateIndexStmt) {
      return (CreateIndexStmt) this.queryTree;
    }
    throw new IllegalStateException(
        "Unknown compiler type: " + this.queryTree.getClass().getSimpleName());
  }

  public boolean isCount() {
    List<SelectItem> items = select().list();
    return items.size() == 1 &&
        items.getFirst().expr() instanceof FunctionCall &&
        ((FunctionCall) items.getFirst().expr()).name().equals("count");
  }

  public Column[] getColumns() {
    if (this.queryTree instanceof CreateTableStmt) {
      return createTable()
          .columns()
          .toArray(Column[]::new);
    } else {
      throw new IllegalStateException(
          "Unknown compiler type: " + this.queryTree.getClass().getSimpleName());
    }
  }
}

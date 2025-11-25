package query_processor;

import java.util.List;
import query_processor.lexer.SqlLexer;
import query_processor.lexer.Token;
import query_processor.parser.SqlParser;
import query_processor.parser.ast.Statement;

public class SqlProcessor {

  public Statement process(String query) {
    SqlLexer lexer = new SqlLexer(query);
    List<Token> tokens = lexer.tokenize();
    SqlParser parser = new SqlParser(tokens);
    return switch (query) {
      case String q when q.toLowerCase().startsWith("select") -> parser.parseSelect();
      case String q when q.toLowerCase().startsWith("create") -> parser.parseCreateStmt();
      default -> throw new IllegalArgumentException("Unknown query: " + query);
    };
  }
}

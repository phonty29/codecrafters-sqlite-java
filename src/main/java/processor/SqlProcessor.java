package processor;

import java.util.List;
import processor.lexer.SqlLexer;
import processor.lexer.Token;
import processor.parser.SqlParser;
import processor.parser.ast.Statement;

public class SqlProcessor {
  public Statement process(String query) {
    SqlLexer lexer = new SqlLexer(query);
    List<Token> tokens = lexer.tokenize();
    SqlParser parser = new SqlParser(tokens);
    return switch (query) {
      case String q when q.startsWith("select") -> parser.parseSelect();
      case String q when q.startsWith("create") -> parser.parseCreateStmt();
      default -> throw new IllegalArgumentException("Unknown query: " + query);
    };
  }
}

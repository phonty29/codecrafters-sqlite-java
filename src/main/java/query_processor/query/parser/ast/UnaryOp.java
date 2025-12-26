package query_processor.query.parser.ast;

import query_processor.Row;

public record UnaryOp(String op, Expression expr) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");
  }
}


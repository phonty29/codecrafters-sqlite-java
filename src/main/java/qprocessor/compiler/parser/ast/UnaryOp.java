package qprocessor.compiler.parser.ast;

import qprocessor.Row;

public record UnaryOp(String op, Expression expr) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");
  }
}


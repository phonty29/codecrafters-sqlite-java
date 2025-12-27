package qprocessor.compiler.parser.ast;

import qprocessor.Row;

public record Identifier(String name) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");
  }
}

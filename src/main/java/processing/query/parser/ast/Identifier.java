package processing.query.parser.ast;

import processing.Row;

public record Identifier(String name) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");
  }
}

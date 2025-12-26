package processing.query.parser.ast;

import processing.Row;

public record Literal(Object value) implements Expression {

  @Override
  public boolean eval(Row row) {
    return (boolean) value;
  }
}

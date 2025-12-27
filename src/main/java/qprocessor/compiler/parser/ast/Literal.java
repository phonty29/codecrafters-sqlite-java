package qprocessor.compiler.parser.ast;

import qprocessor.Row;

public record Literal(Object value) implements Expression {

  @Override
  public boolean eval(Row row) {
    return (boolean) value;
  }
}

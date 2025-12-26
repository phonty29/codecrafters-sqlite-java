package query_processor.query.parser.ast;

import query_processor.Row;

public record Literal(Object value) implements Expression {

  @Override
  public boolean eval(Row row) {
    return (boolean) value;
  }
}

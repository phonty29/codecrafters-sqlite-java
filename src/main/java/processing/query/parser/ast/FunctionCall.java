package processing.query.parser.ast;

import java.util.List;
import processing.Row;

public record FunctionCall(String name, List<Expression> args) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");  }
}

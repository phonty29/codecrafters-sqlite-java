package qprocessor.compiler.parser.ast;

import java.util.List;
import qprocessor.Row;

public record FunctionCall(String name, List<Expression> args) implements Expression {

  @Override
  public boolean eval(Row row) {
    throw new UnsupportedOperationException("Unimplemented method 'eval'");
  }
}

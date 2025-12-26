package processing.query.parser.ast;

import processing.Row;

public interface Expression {
  boolean eval(Row row);
}

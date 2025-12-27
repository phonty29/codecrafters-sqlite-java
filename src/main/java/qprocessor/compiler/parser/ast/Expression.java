package qprocessor.compiler.parser.ast;

import qprocessor.Row;

public interface Expression {
  boolean eval(Row row);
}

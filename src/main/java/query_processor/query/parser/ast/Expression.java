package query_processor.query.parser.ast;

import query_processor.Row;

public interface Expression {
  boolean eval(Row row);
}

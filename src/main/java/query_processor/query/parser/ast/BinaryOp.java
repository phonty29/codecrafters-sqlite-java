package query_processor.query.parser.ast;

import java.util.Objects;
import query_processor.Row;

public record BinaryOp(String op, Expression left, Expression right) implements Expression {

  @Override
  public boolean eval(Row row) {
    return switch (op) {
      case "and" -> left.eval(row) && right.eval(row);
      case "or" -> left.eval(row) || right.eval(row);
      case "=" -> evalEq(row);
      case ">" -> evalGt(row);
      case "<" -> evalLt(row);
      case "<>" -> evalNotEq(row);
      default -> throw new UnsupportedOperationException(op + " is not supported");
    };
  }

  private boolean evalEq(Row row) {
    Identifier col = (Identifier) left;
    Literal val = (Literal) right;
    Object rowValue = row.get(col.name());
    Object condValue = val.value();
    if (Objects.isNull(rowValue) || Objects.isNull(condValue)) {
      return false;
    }
    if (val.value() instanceof String) {
      return condValue.equals(String.valueOf(rowValue));
    }
    if (condValue instanceof Number && rowValue instanceof Number) {
      return ((Number) condValue).doubleValue() == ((Number) rowValue).doubleValue();
    }
    throw new IllegalArgumentException(rowValue + " and " + condValue + " are not comparible objects");
  }

  private boolean evalNotEq(Row row) {
    return !evalEq(row);
  }

  private boolean evalGt(Row row) {
    Identifier col = (Identifier) left;
    var rowValue = row.get(col.name());
    Literal val = (Literal) right;
    return compare(rowValue, val.value()) > 0;
  }

  private boolean evalLt(Row row) {
    Identifier col = (Identifier) left;
    var rowValue = row.get(col.name());
    Literal val = (Literal) right;
    return compare(rowValue, val.value()) < 0;
  }

  private int compare(Object row, Object cond) {
    if (Objects.isNull(row) || Objects.isNull(cond)) {
      return 0;
    }
    if (cond instanceof String) {
      return ((String) cond).compareTo(String.valueOf(row));
    }
    if (cond instanceof Number && row instanceof Number) {
      return Double.compare(((Number) row).doubleValue(), ((Number) cond).doubleValue());
    }
    throw new IllegalArgumentException(row + " and " + cond + " are not comparible objects");
  }
}

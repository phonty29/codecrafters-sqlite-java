package query_processor.parser.ast;

public record BinaryOp(String op, Expression left, Expression right) implements Expression {

}

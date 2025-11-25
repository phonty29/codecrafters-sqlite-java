package query_processor.parser.ast;

public record UnaryOp(String op, Expression expr) implements Expression {

}

